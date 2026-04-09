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


def _load_api_key(api_key_path: str) -> str:
    """Load API key from file or FIREFLIES_API_KEY env var."""
    env_key = os.environ.get("FIREFLIES_API_KEY", "").strip()
    if env_key:
        return env_key

    path = Path(api_key_path)
    if path.is_file():
        key = path.read_text().strip()
        if key:
            return key

    print(
        f"API key not found. Set FIREFLIES_API_KEY env var or create {api_key_path}",
        file=sys.stderr,
    )
    sys.exit(1)


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


async def _run_mount(store: MeetingStore) -> None:
    import pyfuse3
    import trio

    async with trio.open_nursery() as nursery:
        nursery.start_soon(pyfuse3.main)
        nursery.start_soon(_backfill_cache, store)


def cmd_mount(args: argparse.Namespace) -> None:
    """Mount the FUSE filesystem."""
    import pyfuse3
    import trio

    from .api import FirefliesClient
    from .fuse_ops import FirefliesMeetingOps
    from .status_cache import StatusCache
    from .store import MeetingStore

    api_key = _load_api_key(args.api_key)
    mountpoint = Path(args.mountpoint)
    mountpoint.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    client = FirefliesClient(api_key)
    status_cache = StatusCache()

    user_email = os.environ.get("FIREFLIES_USER_EMAIL", "").strip()
    if not user_email:
        user_email = client.get_user_email() or ""
        if user_email:
            logging.getLogger(__name__).info("Resolved user email from API: %s", user_email)
        else:
            logging.getLogger(__name__).warning(
                "Could not determine user email; mine/ directory will be unavailable"
            )

    store = MeetingStore(client, status_cache=status_cache, user_email=user_email or None)
    ops = FirefliesMeetingOps(store)

    fuse_options: set[str] = {"fsname=fireflies-meetings", "ro"}
    if args.debug:
        fuse_options.add("debug")

    def _handle_usr1(signum: int, frame: object) -> None:
        store.force_refresh()

    signal.signal(signal.SIGUSR1, _handle_usr1)

    pyfuse3.init(ops, str(mountpoint), fuse_options)

    try:
        trio.run(_run_mount, store)
    except KeyboardInterrupt:
        pass
    finally:
        pyfuse3.close()
        client.close()


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
    mount_parser.set_defaults(func=cmd_mount)

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
