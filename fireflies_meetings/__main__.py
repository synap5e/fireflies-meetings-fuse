"""CLI entry point for fireflies-meetings FUSE filesystem."""

from __future__ import annotations

import argparse
import functools
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .store import MeetingStore


def _default_mountpoint() -> str:
    return os.path.expanduser("~/views/fireflies-meetings")


def _default_api_key_path() -> str:
    return os.path.expanduser("~/.config/fireflies-meetings/api_key")


def _default_chat_token_path() -> str:
    return os.path.expanduser("~/.config/fireflies-meetings/google_chat_token.json")


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

    from .chat_watcher import ChatWatcher, load_credentials

    creds = load_credentials(token_path)
    if creds is None:
        _log.warning(
            "Google Chat token at %s is missing or invalid; "
            "live meeting discovery disabled. Run `fireflies-meetings auth-chat` to set it up.",
            token_path,
        )
        return

    watcher = ChatWatcher(creds, token_path=token_path)
    _log.info("Google Chat watcher started (polling every %.0fs)", poll_interval)
    try:
        while True:
            try:
                ids = await trio.to_thread.run_sync(
                    functools.partial(watcher.find_live_meeting_ids, lookback_seconds=lookback_seconds),
                )
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


async def _run_mount(store: MeetingStore, chat_token_path: Path | None) -> None:
    import pyfuse3
    import trio

    async with trio.open_nursery() as nursery:
        nursery.start_soon(pyfuse3.main)
        nursery.start_soon(_backfill_cache, store)
        nursery.start_soon(_signal_listener, store)
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

    session_token = os.environ.get("FIREFLIES_SESSION_TOKEN", "").strip() or None
    client = FirefliesClient(api_key, session_token=session_token)
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
            trio.run(_run_mount, store, chat_token_arg)
        except KeyboardInterrupt:
            pass
        finally:
            pyfuse3.close()
    finally:
        client.close()


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
