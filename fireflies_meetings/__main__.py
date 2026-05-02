"""CLI entry point for fireflies-meetings FUSE filesystem."""

from __future__ import annotations

import argparse
import functools
import json
import logging
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .api import FirefliesClient
    from .session_auth import SessionAuth
    from .store import MeetingStore


def _default_mountpoint() -> str:
    return "/views/fireflies-meetings"


def _default_api_key_path() -> str:
    return os.path.expanduser("~/.config/fireflies-meetings/api_key")


def _default_chat_token_path() -> str:
    return os.path.expanduser("~/.config/fireflies-meetings/google_chat_token.json")


def _default_session_auth_path() -> str:
    return os.path.expanduser("~/.config/fireflies-meetings/session.json")


def _resolve_chat_credentials(explicit: str | None) -> Path | None:
    """Find the Google Chat OAuth client-secret JSON.

    Order: --chat-credentials arg, project-local `secrets/client_secret_*.json`
    (relative to CWD), ~/.config/fireflies-meetings/google_chat_credentials.json.
    """
    if explicit:
        p = Path(explicit).expanduser()
        return p if p.exists() else None
    local = sorted(Path("secrets").glob("client_secret_*.json"))
    if local:
        return local[0]
    home = Path.home() / ".config" / "fireflies-meetings" / "google_chat_credentials.json"
    return home if home.exists() else None


def _load_api_key(api_key_path: str) -> str:
    """Load API key from file or FIREFLIES_API_KEY env var."""
    env_key = os.environ.get("FIREFLIES_API_KEY", "").strip()
    if env_key:
        return env_key

    path = Path(api_key_path)
    if path.is_file():
        try:
            mode = path.stat().st_mode & 0o777
            if mode & 0o077:
                print(
                    f"warning: {api_key_path} is readable by group/other (mode {mode:o}). "
                    f"Run: chmod 600 {api_key_path}",
                    file=sys.stderr,
                )
            key = path.read_text().strip()
        except OSError as e:
            print(
                f"Failed to read API key file {api_key_path}: {e}",
                file=sys.stderr,
            )
            sys.exit(1)
        if key:
            return key

    print(
        f"API key not found. Set FIREFLIES_API_KEY env var or create {api_key_path}",
        file=sys.stderr,
    )
    sys.exit(1)


def _configure_logging(*, debug: bool) -> None:
    """Set log levels with care: enable DEBUG on our package only.

    httpx and httpcore log raw request/response bytes (including the
    Authorization header) at DEBUG. Setting the *root* logger to DEBUG
    would publish the bearer token to the systemd journal. Instead we:

    - Leave the root logger at INFO regardless
    - Bump our own package to DEBUG when --debug is set
    - Clamp httpx/httpcore/h11/hpack to WARNING explicitly
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if debug:
        logging.getLogger("fireflies_meetings").setLevel(logging.DEBUG)
    for noisy in ("httpx", "httpcore", "h11", "hpack"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


_log = logging.getLogger(__name__)


def _load_session_auth(path_str: str) -> SessionAuth | None:
    from .session_auth import SessionAuth

    env_auth = SessionAuth.from_env()
    if env_auth is not None:
        return env_auth

    path = Path(path_str).expanduser()
    refreshed = _refresh_session_auth_from_browser(path)
    if refreshed is not None:
        return refreshed

    if not path.exists():
        return None
    try:
        return SessionAuth.load(path)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        _log.warning("Failed to load Fireflies session auth from %s: %s", path, e)
        return None


def _refresh_session_auth_from_browser(path: Path) -> SessionAuth | None:
    from .browser_auth import refresh_session_auth

    browser = os.environ.get("FIREFLIES_SESSION_BROWSER", "chrome").strip() or "chrome"
    profile = os.environ.get("FIREFLIES_SESSION_PROFILE", "Default").strip() or "Default"
    try:
        session_auth = refresh_session_auth(
            path,
            browser=browser,
            profile=profile,
            open_login=False,
            wait_timeout=0.0,
        )
    except RuntimeError as e:
        _log.debug(
            "Non-interactive Fireflies browser-session refresh unavailable for %s/%s: %s",
            browser,
            profile,
            e,
        )
        return None

    _log.info(
        "Refreshed Fireflies session auth from %s browser profile %s",
        browser,
        profile,
    )
    return session_auth


def _restart_service_if_running() -> None:
    """Restart the systemd user service so a refreshed web session is picked up."""
    status = subprocess.run(
        ["systemctl", "--user", "is-active", "--quiet", "fireflies-meetings"],
        capture_output=True,
        text=True,
    )
    if status.returncode != 0:
        return

    restart = subprocess.run(
        ["systemctl", "--user", "restart", "fireflies-meetings"],
        capture_output=True,
        text=True,
    )
    if restart.returncode == 0:
        print("Restarted fireflies-meetings service to pick up the refreshed web session.")
    else:
        print(
            "Refreshed the session file, but failed to restart fireflies-meetings: "
            f"{restart.stderr.strip()}",
            file=sys.stderr,
        )


async def _backfill_cache(store: MeetingStore) -> None:
    import httpx
    import trio

    from .api import FatalAPIError, RateLimitedError, TransientAPIError

    await trio.sleep(5)  # let pyfuse3.main settle first
    while True:
        pending = store.get_uncached_meeting_ids()
        if not pending:
            await trio.sleep(60)
            continue
        for mid in pending:
            try:
                await trio.to_thread.run_sync(functools.partial(store.backfill_one, mid))
                _log.debug("Backfilled %s", mid)
            except RateLimitedError as e:
                wait = e.retry_after if e.retry_after else 60.0
                _log.info("Rate limited during backfill; sleeping %.0fs", wait)
                await trio.sleep(wait)
            except FatalAPIError:
                _log.warning("Fatal API error during backfill; stopping")
                return
            except TransientAPIError as e:
                _log.warning("Transient error backfilling %s; skipping: %s", mid, e)
            except (OSError, httpx.HTTPError, ValueError):
                _log.exception("Unexpected error backfilling %s; skipping", mid)
            await trio.sleep(3)  # ~20 fetches/min, well under 60/min rate limit


async def _signal_listener(store: MeetingStore) -> None:
    """Trio-native SIGUSR1 handler. Replaces signal.signal so the refresh
    is dispatched cleanly through the trio loop instead of from arbitrary
    interrupt context."""
    import trio

    with trio.open_signal_receiver(signal.SIGUSR1) as receiver:
        async for _signum in receiver:
            _log.info("SIGUSR1 received — forcing refresh")
            await trio.to_thread.run_sync(store.force_refresh)


async def _chat_watch_loop(
    store: MeetingStore,
    token_path: Path,
    *,
    poll_interval: float = 30.0,
    lookback_seconds: float = 7 * 86400.0,
) -> None:
    """Poll Google Chat for Fireflies live-meeting URLs and register IDs.

    The Fireflies `transcripts` list query hides live meetings from
    non-admin users. This loop reads the user's Chat spaces for messages
    containing `app.fireflies.ai/live/<id>` URLs (posted by Meet when the
    Fireflies bot joins) and feeds those IDs into `store.watch_meeting`.
    """
    import httpx
    import trio

    from .chat_watcher import ChatAuthExpiredError, ChatWatcher, load_credentials

    creds = load_credentials(token_path)
    if creds is None:
        _log.warning(
            "Google Chat token at %s is missing or invalid; "
            "live meeting discovery disabled. Run `fireflies-meetings auth-chat` to set it up.",
            token_path,
        )
        store.mark_chat_auth_fatal()
        return

    watcher = ChatWatcher(creds, token_path=token_path)
    _log.info("Google Chat watcher started (polling every %.0fs)", poll_interval)
    try:
        while True:
            try:
                ids = await trio.to_thread.run_sync(
                    functools.partial(watcher.find_live_meeting_ids, lookback_seconds=lookback_seconds),
                )
            except ChatAuthExpiredError as e:
                _log.warning(
                    "Google Chat credentials unusable: %s. "
                    "Live meeting discovery disabled until `fireflies-meetings auth-chat` "
                    "is re-run and the service is restarted.",
                    e,
                )
                store.mark_chat_auth_fatal()
                return
            except (OSError, httpx.HTTPError, ValueError):
                _log.exception("Chat watcher poll failed; retrying next interval")
                await trio.sleep(poll_interval)
                continue

            if ids:
                _log.debug("Chat watcher found %d candidate meeting IDs", len(ids))
            for mid in ids:
                await trio.to_thread.run_sync(store.watch_meeting, mid)
            await trio.sleep(poll_interval)
    finally:
        watcher.close()


async def _live_transcript_stream_loop(
    store: MeetingStore,
    client: FirefliesClient,
    meeting_id: str,
    stop_event: threading.Event,
) -> None:
    import trio

    from .live_stream import LiveTranscriptStreamError, stream_live_transcript

    _log.info("Live transcript stream started for %s", meeting_id)
    while not stop_event.is_set():
        try:
            await trio.to_thread.run_sync(
                functools.partial(
                    stream_live_transcript,
                    client,
                    meeting_id,
                    on_update=functools.partial(store.apply_live_transcript_update, meeting_id),
                    stop_event=stop_event,
                ),
            )
        except LiveTranscriptStreamError:
            if stop_event.is_set():
                break
            _log.exception("Live transcript stream failed for %s; retrying", meeting_id)
            await trio.sleep(3)
            continue

        if not stop_event.is_set():
            await trio.sleep(1)

    _log.info("Live transcript stream stopped for %s", meeting_id)


async def _poll_active_meeting_ids(
    client: FirefliesClient,
    poll_interval: float,
) -> list[str] | None:
    import httpx
    import trio

    from .api import FatalAPIError, RateLimitedError, TransientAPIError

    try:
        return await trio.to_thread.run_sync(client.list_active_meeting_ids)
    except RateLimitedError as e:
        wait = e.retry_after if e.retry_after else poll_interval
        _log.info("Rate limited during active meeting discovery; sleeping %.0fs", wait)
        await trio.sleep(wait)
        return None
    except FatalAPIError:
        raise
    except TransientAPIError as e:
        _log.warning("Transient error during active meeting discovery: %s", e)
        await trio.sleep(poll_interval)
        return None
    except (OSError, httpx.HTTPError, ValueError):
        _log.exception("Active meeting discovery failed; retrying next interval")
        await trio.sleep(poll_interval)
        return None


def _stop_live_transcript_streams(stream_stops: dict[str, threading.Event]) -> None:
    for stop_event in stream_stops.values():
        stop_event.set()


async def _active_meetings_watch_loop(
    store: MeetingStore,
    client: FirefliesClient,
    *,
    poll_interval: float = 30.0,
) -> None:
    """Poll Fireflies' official active_meetings query for the user's live meetings."""
    import trio

    from .api import FatalAPIError

    _log.info("Active meetings watcher started (polling every %.0fs)", poll_interval)
    stream_stops: dict[str, threading.Event] = {}
    stream_enabled = client.has_internal_auth
    if not stream_enabled:
        _log.info("Live transcript streaming disabled; no internal Fireflies session auth configured")

    async with trio.open_nursery() as nursery:
        while True:
            try:
                ids = await _poll_active_meeting_ids(client, poll_interval)
            except FatalAPIError:
                _log.warning("Fatal API error during active meeting discovery; stopping")
                _stop_live_transcript_streams(stream_stops)
                return

            if ids is None:
                continue

            await trio.to_thread.run_sync(store.sync_active_meeting_ids, ids)
            active_ids = set(ids)
            for mid in ids:
                if not await trio.to_thread.run_sync(store.watch_meeting, mid):
                    continue
                if not stream_enabled or mid in stream_stops:
                    continue

                stop_event = threading.Event()
                stream_stops[mid] = stop_event
                nursery.start_soon(_live_transcript_stream_loop, store, client, mid, stop_event)

            for mid in list(stream_stops):
                if mid in active_ids:
                    continue
                stream_stops[mid].set()
                del stream_stops[mid]

            await trio.sleep(poll_interval)


async def _run_mount(
    store: MeetingStore,
    client: FirefliesClient,
    chat_token_path: Path | None,
) -> None:
    import pyfuse3
    import trio

    async with trio.open_nursery() as nursery:
        nursery.start_soon(pyfuse3.main)
        nursery.start_soon(_backfill_cache, store)
        nursery.start_soon(_signal_listener, store)
        nursery.start_soon(_active_meetings_watch_loop, store, client)
        if chat_token_path is not None:
            nursery.start_soon(_chat_watch_loop, store, chat_token_path)


def cmd_mount(args: argparse.Namespace) -> None:
    """Mount the FUSE filesystem."""
    import pyfuse3
    import trio

    from .api import FirefliesClient
    from .fuse_ops import FirefliesMeetingOps
    from .status_cache import StatusCache
    from .store import MeetingStore

    _configure_logging(debug=args.debug)

    api_key = _load_api_key(args.api_key)
    mountpoint = Path(args.mountpoint)
    mountpoint.mkdir(parents=True, exist_ok=True)

    session_auth = _load_session_auth(args.session_auth)
    client = FirefliesClient(api_key, session_auth=session_auth)
    try:
        status_cache = StatusCache()

        user_email = os.environ.get("FIREFLIES_USER_EMAIL", "").strip()
        if not user_email:
            user_email = client.get_user_email() or ""
            if user_email:
                _log.info("Resolved user email from API: %s", user_email)
            else:
                _log.warning(
                    "Could not determine user email; mine/ directory will be unavailable"
                )

        store = MeetingStore(client, status_cache=status_cache, user_email=user_email or None)
        ops = FirefliesMeetingOps(store)

        fuse_options: set[str] = {
            "fsname=fireflies-meetings",
            "ro",
            "nosuid",
            "nodev",
            "allow_root",
        }
        if args.debug:
            fuse_options.add("debug")

        chat_token_path = Path(args.chat_token).expanduser()
        chat_token_arg: Path | None = chat_token_path if chat_token_path.exists() else None
        if chat_token_arg is None:
            _log.info(
                "No Google Chat token at %s; live meeting discovery disabled.",
                chat_token_path,
            )

        pyfuse3.init(ops, str(mountpoint), fuse_options)
        try:
            trio.run(_run_mount, store, client, chat_token_arg)
        except KeyboardInterrupt:
            pass
        finally:
            pyfuse3.close()
    finally:
        client.close()


def cmd_auth_session(args: argparse.Namespace) -> None:
    """Import a valid Fireflies web session from the browser and save it to disk."""
    from .browser_auth import refresh_session_auth

    _configure_logging(debug=False)
    dest_path = Path(args.session_auth).expanduser()
    try:
        session_auth = refresh_session_auth(
            dest_path,
            browser=args.browser,
            profile=args.profile,
            open_login=not args.no_open_browser,
            wait_timeout=args.wait_timeout,
        )
    except RuntimeError as e:
        print(f"Failed to refresh Fireflies web session: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Saved Fireflies web session to {dest_path}")
    if session_auth.refresh_token:
        print("Session includes refresh token (x-cache) for the internal live-caption fallback.")
    _restart_service_if_running()


def cmd_auth_chat(args: argparse.Namespace) -> None:
    """Run the Google Chat OAuth flow and save the token."""
    from .chat_watcher import authorize

    _configure_logging(debug=False)
    creds_path = _resolve_chat_credentials(args.chat_credentials)
    if creds_path is None:
        print(
            "Google Chat credentials not found. Place the OAuth client_secret JSON at "
            "./secrets/client_secret_*.json or ~/.config/fireflies-meetings/google_chat_credentials.json, "
            "or pass --chat-credentials PATH.",
            file=sys.stderr,
        )
        sys.exit(1)

    token_path = Path(args.chat_token).expanduser()
    print(f"Using credentials: {creds_path}")
    print(f"Token will be saved to: {token_path}")
    print("Opening a browser for OAuth authorization...")
    authorize(creds_path, token_path)
    print(f"Success. Token saved to {token_path}")


def cmd_unmount(args: argparse.Namespace) -> None:
    """Unmount the FUSE filesystem."""
    mountpoint = args.mountpoint
    result = subprocess.run(
        ["fusermount3", "-u", mountpoint],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Failed to unmount: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    print(f"Unmounted {mountpoint}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="fireflies-meetings",
        description="FUSE filesystem for Fireflies.ai meeting data",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    mount_parser = sub.add_parser("mount", help="Mount the filesystem")
    mount_parser.add_argument(
        "mountpoint",
        nargs="?",
        default=_default_mountpoint(),
        help=f"Mount point (default: {_default_mountpoint()})",
    )
    mount_parser.add_argument(
        "--api-key",
        default=_default_api_key_path(),
        help=f"Path to API key file (default: {_default_api_key_path()})",
    )
    mount_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable FUSE debug output",
    )
    mount_parser.add_argument(
        "--chat-token",
        default=_default_chat_token_path(),
        help=(
            f"Path to saved Google Chat OAuth token (default: {_default_chat_token_path()}). "
            "If missing, live meeting discovery is disabled."
        ),
    )
    mount_parser.add_argument(
        "--session-auth",
        default=_default_session_auth_path(),
        help=(
            f"Path to Fireflies web-session JSON used for internal live-caption fallback "
            f"(default: {_default_session_auth_path()})."
        ),
    )
    mount_parser.set_defaults(func=cmd_mount)

    auth_chat_parser = sub.add_parser(
        "auth-chat",
        help="Run Google Chat OAuth flow (enables live meeting discovery)",
    )
    auth_chat_parser.add_argument(
        "--chat-credentials",
        default=None,
        help=(
            "Path to Google Chat OAuth client_secret JSON. "
            "Default: first match of ./secrets/client_secret_*.json, "
            "then ~/.config/fireflies-meetings/google_chat_credentials.json"
        ),
    )
    auth_chat_parser.add_argument(
        "--chat-token",
        default=_default_chat_token_path(),
        help=f"Path to save the token (default: {_default_chat_token_path()})",
    )
    auth_chat_parser.set_defaults(func=cmd_auth_chat)

    auth_session_parser = sub.add_parser(
        "auth-session",
        help="Refresh the Fireflies web session used for live transcript fallback",
    )
    auth_session_parser.add_argument(
        "--session-auth",
        default=_default_session_auth_path(),
        help=f"Path to save the session JSON (default: {_default_session_auth_path()})",
    )
    auth_session_parser.add_argument(
        "--browser",
        choices=("chrome", "chromium"),
        default="chrome",
        help="Browser profile to read cookies from (default: chrome)",
    )
    auth_session_parser.add_argument(
        "--profile",
        default="Default",
        help="Browser profile directory name (default: Default)",
    )
    auth_session_parser.add_argument(
        "--no-open-browser",
        action="store_true",
        help="Only import the current browser session; do not open the Fireflies login page",
    )
    auth_session_parser.add_argument(
        "--wait-timeout",
        type=float,
        default=180.0,
        help="Seconds to wait for login if a fresh browser session is needed (default: 180)",
    )
    auth_session_parser.set_defaults(func=cmd_auth_session)

    unmount_parser = sub.add_parser("unmount", help="Unmount the filesystem")
    unmount_parser.add_argument(
        "mountpoint",
        nargs="?",
        default=_default_mountpoint(),
        help=f"Mount point (default: {_default_mountpoint()})",
    )
    unmount_parser.set_defaults(func=cmd_unmount)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
