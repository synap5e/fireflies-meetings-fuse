"""pyfuse3 Operations subclass for the Fireflies meetings filesystem."""

from __future__ import annotations

import errno
import logging
import os
import stat
import sys
import threading
import time
from collections.abc import Sequence
from pathlib import Path

import pyfuse3
import trio

from .inode_map import InodeMap
from .store import MEETING_FILES, MeetingEntry, MeetingStore

log = logging.getLogger(__name__)


def _resolve_cli_path() -> str:
    """Best-effort absolute path to the `fireflies-meetings` entry point.

    Uses the running interpreter's bin dir so the marker text copy-pastes
    into a shell without activating the venv. Falls back to the bare name
    if the file isn't adjacent to the interpreter.
    """
    candidate = Path(sys.executable).parent / "fireflies-meetings"
    return str(candidate) if candidate.exists() else "fireflies-meetings"


def _resolve_env_path() -> str:
    """Best-effort absolute path to the service's .env.

    The systemd unit uses %h/agentic/fireflies-meetings-fuse/.env. In an
    editable install this module lives inside that tree, so we can infer
    the project root from __file__. Falls back to a sensible default.
    """
    candidate = Path(__file__).resolve().parents[1] / ".env"
    return str(candidate) if candidate.exists() else "~/agentic/fireflies-meetings-fuse/.env"


def _resolve_chat_credentials_project_glob() -> str:
    """Best-effort absolute glob for project-local Google OAuth secrets."""
    return str(Path(__file__).resolve().parents[1] / "secrets" / "client_secret_*.json")


def _resolve_chat_credentials_home_path() -> str:
    """Absolute fallback path for Google OAuth client secrets."""
    return str(Path.home() / ".config" / "fireflies-meetings" / "google_chat_credentials.json")


def _resolve_chat_token_path() -> str:
    """Absolute path for the persisted Google Chat OAuth token."""
    return str(Path.home() / ".config" / "fireflies-meetings" / "google_chat_token.json")


_CLI_PATH = _resolve_cli_path()
_ENV_PATH = _resolve_env_path()
_CHAT_CREDENTIALS_PROJECT_GLOB = _resolve_chat_credentials_project_glob()
_CHAT_CREDENTIALS_HOME_PATH = _resolve_chat_credentials_home_path()
_CHAT_TOKEN_PATH = _resolve_chat_token_path()

# Path structure (non-mine):
#   depth 0  /
#   depth 1  /2026-03/
#   depth 2  /2026-03/25/
#   depth 3  /2026-03/25/backend-q1-retrospective/
#   depth 4  /2026-03/25/backend-q1-retrospective/summary.md
#   depth 4  /2026-03/25/backend-q1-retrospective/ghost/
#   depth 5  /2026-03/25/backend-q1-retrospective/ghost/summary.md
#   depth 4  /2026-03/25/backend-q1-retrospective/overlap/
#   depth 5  /2026-03/25/backend-q1-retrospective/overlap/summary.md
#
# mine/ subtree adds one level at the front:
#   depth 1  /mine/
#   depth 2  /mine/2026-03/
#   ...depth N+1 mirrors depth N above

_MINE_DIR = "mine"
_LIVE_DIR = "live"
_EXECUTABLE_NAMES = frozenset({"open.sh"})
_AUTH_EXPIRED_NAME = "AUTHENTICATION_EXPIRED"
_AUTH_EXPIRED_CONTENT = (
    f"""\
Fireflies API authentication failed (401/403). All background fetches are
stopped until the service is restarted with working credentials.

To recover:

    1. Get a fresh API key from:

           https://app.fireflies.ai/integrations/custom/fireflies

       Paste it as the FIREFLIES_API_KEY value in:

           {_ENV_PATH}

       (or write it to ~/.config/fireflies-meetings/api_key as one line.)

    2. If the internal live-caption fallback also needs a refresh
       (FIREFLIES_SESSION_TOKEN, session.json), run:

           {_CLI_PATH} auth-session

       This scrapes the session cookie from your logged-in browser and
       auto-restarts the service on success.

    3. systemctl --user restart fireflies-meetings

Do NOT run `{_CLI_PATH} mount` manually while the systemd service
still owns the mountpoint -- that only starts a second, failing mount.
"""
).encode("ascii")

_CHAT_AUTH_EXPIRED_NAME = "CHAT_AUTH_EXPIRED"
_CHAT_AUTH_EXPIRED_CONTENT = (
    f"""\
Google Chat credentials are missing or revoked. Live meeting discovery via
Chat is disabled. (The Fireflies API side may still be working -- see
AUTHENTICATION_EXPIRED if that file is also present.)

To recover:

    1. Put your Google OAuth client-secrets JSON at one of:

           {_CHAT_CREDENTIALS_PROJECT_GLOB}

       or:

           {_CHAT_CREDENTIALS_HOME_PATH}

    2. Run:

           {_CLI_PATH} auth-chat

       or explicitly:

           {_CLI_PATH} auth-chat --chat-credentials /absolute/path/to/client_secret_....json

       The refreshed chat token will be written to:

           {_CHAT_TOKEN_PATH}

    3. systemctl --user restart fireflies-meetings
"""
).encode("ascii")

# sub_depth = number of path components after stripping the optional "mine/" prefix.
# Directories have sub_depth 0-3, plus ghost/overlap directories at sub_depth 4.
# Meeting files have sub_depth 4; ghost/overlap files have sub_depth 5.
_SUB_DEPTH_MONTH = 1
_SUB_DEPTH_DAY = 2
_SUB_DEPTH_MEETING = 3
_SUB_DEPTH_FILE = 4
_SUB_DEPTH_GHOST_DIR = 4
_SUB_DEPTH_GHOST_FILE = 5
_SUB_DEPTH_OVERLAP_DIR = 4
_SUB_DEPTH_OVERLAP_FILE = 5
_DYNAMIC_TIMEOUT = 0.0
_STATIC_TIMEOUT = 300.0
_IN_PROGRESS_FILE = "_in_progress"
_OVERLAP_WARNING_FILE = "_overlap_warning.md"
_OVERLAP_WARNING_SIZE = 512


def _now_ns() -> int:
    return int(time.time() * 1e9)


def _make_dir_attr(inode: int, *, timeout: float = _STATIC_TIMEOUT) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = pyfuse3.InodeT(inode)
    entry.st_mode = stat.S_IFDIR | 0o555
    entry.st_nlink = 2
    entry.st_size = 0
    entry.attr_timeout = timeout
    entry.entry_timeout = timeout
    now = _now_ns()
    entry.st_atime_ns = now
    entry.st_mtime_ns = now
    entry.st_ctime_ns = now
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _make_symlink_attr(
    inode: int,
    target_len: int,
    *,
    timeout: float = _STATIC_TIMEOUT,
) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = pyfuse3.InodeT(inode)
    entry.st_mode = stat.S_IFLNK | 0o777
    entry.st_nlink = 1
    entry.st_size = target_len
    entry.attr_timeout = timeout
    entry.entry_timeout = timeout
    now = _now_ns()
    entry.st_atime_ns = now
    entry.st_mtime_ns = now
    entry.st_ctime_ns = now
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _make_file_attr(
    inode: int, size: int, *, executable: bool = False,
    timeout: float = _STATIC_TIMEOUT,
) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = pyfuse3.InodeT(inode)
    entry.st_mode = stat.S_IFREG | (0o555 if executable else 0o444)
    entry.st_nlink = 1
    entry.st_size = size
    entry.attr_timeout = timeout
    entry.entry_timeout = timeout
    now = _now_ns()
    entry.st_atime_ns = now
    entry.st_mtime_ns = now
    entry.st_ctime_ns = now
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _parse_path(path: str) -> tuple[bool, list[str]]:
    """Return (is_mine, sub_parts) after stripping an optional leading 'mine' component."""
    if path == "/":
        return False, []
    parts = path.strip("/").split("/")
    if parts[0] == _MINE_DIR:
        return True, parts[1:]
    return False, parts


def _looks_like_overlap_name(name: str) -> bool:
    if name == "overlap":
        return True
    prefix = "overlap-"
    if not name.startswith(prefix):
        return False
    try:
        return int(name.removeprefix(prefix)) >= 2
    except ValueError:
        return False


class FirefliesMeetingOps(pyfuse3.Operations):
    """Read-only FUSE operations for Fireflies.ai meetings."""

    def __init__(self, store: MeetingStore) -> None:
        super().__init__()
        self._store = store
        self._inodes = InodeMap()
        self._content: dict[int, bytes] = {}
        self._dynamic_fhs: set[int] = set()
        self._symlinks: dict[int, bytes] = {}  # inode -> symlink target bytes
        self._state_lock = threading.Lock()
        self._store.set_live_change_callback(self._invalidate_meeting)

    def _get_path(self, inode: int) -> str | None:
        with self._state_lock:
            return self._inodes.get_path(inode)

    def _get_or_create_inode(self, path: str) -> int:
        with self._state_lock:
            return self._inodes.get_or_create(path)

    def _get_inode(self, path: str) -> int | None:
        with self._state_lock:
            return self._inodes.get_inode(path)

    def _remember_symlink(self, inode: int, target: bytes) -> None:
        with self._state_lock:
            self._symlinks[inode] = target

    def _remember_static_content(self, inode: int, content: bytes) -> None:
        with self._state_lock:
            self._content[inode] = content
            self._dynamic_fhs.discard(inode)

    def _mark_dynamic_handle(self, inode: int) -> None:
        with self._state_lock:
            self._content.pop(inode, None)
            self._dynamic_fhs.add(inode)

    def _is_dynamic_handle(self, inode: int) -> bool:
        with self._state_lock:
            return inode in self._dynamic_fhs

    def _get_static_content(self, inode: int) -> bytes | None:
        with self._state_lock:
            return self._content.get(inode)

    def _release_handle(self, inode: int) -> None:
        with self._state_lock:
            self._content.pop(inode, None)
            self._dynamic_fhs.discard(inode)

    def _forget_inode(self, inode: int, nlookup: int) -> None:
        with self._state_lock:
            self._inodes.forget(inode, nlookup)
            if self._inodes.get_path(inode) is None:
                self._content.pop(inode, None)
                self._dynamic_fhs.discard(inode)
                self._symlinks.pop(inode, None)

    def _is_dynamic_path(self, path: str) -> bool:
        if path == f"/{_LIVE_DIR}":
            return True

        is_mine, sub = _parse_path(path)
        if not is_mine and len(sub) == 2 and sub[0] == _LIVE_DIR:
            return True
        if len(sub) >= _SUB_DEPTH_GHOST_DIR and sub[3] == "ghost":
            return False
        if len(sub) >= _SUB_DEPTH_OVERLAP_DIR and (
            sub[3] == _OVERLAP_WARNING_FILE or _looks_like_overlap_name(sub[3])
        ):
            return False
        if len(sub) not in {_SUB_DEPTH_MEETING, _SUB_DEPTH_FILE}:
            return False

        year_month, day, slug = sub[:3]
        date_str = f"{year_month}-{day}"
        try:
            meetings = (
                self._store.list_meetings_mine(date_str) if is_mine
                else self._store.list_meetings(date_str)
            )
            entry = meetings.get(slug)
        except Exception:
            log.exception("Error resolving dynamic path %s", path)
            return False
        if entry is None:
            return False
        return self._store.is_meeting_dynamic(entry.meeting.id)

    def _timeout_for_path(self, path: str) -> float:
        _is_mine, sub = _parse_path(path)
        if len(sub) >= _SUB_DEPTH_GHOST_DIR and sub[3] == "ghost":
            return _STATIC_TIMEOUT
        if len(sub) >= _SUB_DEPTH_OVERLAP_DIR and (
            sub[3] == _OVERLAP_WARNING_FILE or _looks_like_overlap_name(sub[3])
        ):
            return _STATIC_TIMEOUT
        return _DYNAMIC_TIMEOUT if self._is_dynamic_path(path) else _STATIC_TIMEOUT

    def _invalidate_meeting(self, meeting_id: str) -> None:
        paths = self._store.get_meeting_paths(meeting_id)
        if paths is None:
            return

        meeting_dir, live_path, mine_path = paths
        candidate_paths = [meeting_dir, "/live"]
        for optional_path in (live_path, mine_path):
            if optional_path is not None:
                candidate_paths.append(optional_path)

        for filename in (*self._store.list_files(meeting_id), _IN_PROGRESS_FILE):
            candidate_paths.append(f"{meeting_dir}/{filename}")
            if mine_path is not None:
                candidate_paths.append(f"{mine_path}/{filename}")

        seen_inodes: set[int] = set()
        for path in candidate_paths:
            inode = self._get_inode(path)
            if inode is None or inode in seen_inodes:
                continue
            seen_inodes.add(inode)
            try:
                pyfuse3.invalidate_inode(pyfuse3.InodeT(inode))
            except OSError:
                log.debug("Skipping inode invalidation for %s", path, exc_info=True)

    def _resolve_meeting_entry(self, sub: list[str], is_mine: bool) -> MeetingEntry | None:
        if len(sub) < _SUB_DEPTH_MEETING:
            return None
        year_month, day, slug = sub[:3]
        date_str = f"{year_month}-{day}"
        try:
            meetings = (
                self._store.list_meetings_mine(date_str) if is_mine
                else self._store.list_meetings(date_str)
            )
        except Exception:
            log.exception("Error listing meetings for %s", date_str)
            return None
        return meetings.get(slug)

    def _resolve_ghost_parent_id(self, sub: list[str], is_mine: bool) -> str | None:
        if len(sub) < _SUB_DEPTH_GHOST_DIR or sub[3] != "ghost":
            return None
        entry = self._resolve_meeting_entry(sub, is_mine)
        if entry is None:
            return None
        meeting_id = entry.meeting.id
        return meeting_id if self._store.get_ghost_id(meeting_id) is not None else None

    def _resolve_ghost_file(self, path: str) -> tuple[str, str, str] | None:
        is_mine, sub = _parse_path(path)
        if len(sub) != _SUB_DEPTH_GHOST_FILE or sub[4] not in MEETING_FILES:
            return None
        meeting_id = self._resolve_ghost_parent_id(sub, is_mine)
        if meeting_id is None:
            return None
        return meeting_id, sub[2], sub[4]

    def _resolve_overlap_parent_id(self, sub: list[str], is_mine: bool) -> str | None:
        if len(sub) < _SUB_DEPTH_OVERLAP_DIR:
            return None
        entry = self._resolve_meeting_entry(sub, is_mine)
        if entry is None:
            return None
        meeting_id = entry.meeting.id
        overlap_dirname = sub[3]
        overlap_id = self._store.get_overlap_id_for_dirname(meeting_id, overlap_dirname)
        return meeting_id if overlap_id is not None else None

    def _resolve_overlap_file(self, path: str) -> tuple[str, str, str, str] | None:
        is_mine, sub = _parse_path(path)
        if len(sub) != _SUB_DEPTH_OVERLAP_FILE or sub[4] not in MEETING_FILES:
            return None
        meeting_id = self._resolve_overlap_parent_id(sub, is_mine)
        if meeting_id is None:
            return None
        return meeting_id, sub[2], sub[3], sub[4]

    def _resolve_overlap_warning_file(self, path: str) -> tuple[str, str] | None:
        is_mine, sub = _parse_path(path)
        if len(sub) != _SUB_DEPTH_FILE or sub[3] != _OVERLAP_WARNING_FILE:
            return None
        entry = self._resolve_meeting_entry(sub, is_mine)
        if entry is None:
            return None
        meeting_id = entry.meeting.id
        if not self._store.get_overlap_ids(meeting_id):
            return None
        return meeting_id, sub[2]

    def _resolve_meeting_file(self, path: str) -> tuple[str, str, str] | None:
        """Parse a path into (meeting_id, slug, filename) if it's a meeting file.

        Returns None for non-file paths or paths that don't match a known meeting.
        Cheap: only consults the in-memory `_store.list_meetings*` snapshots.
        """
        is_mine, sub = _parse_path(path)
        if len(sub) != _SUB_DEPTH_FILE:
            return None

        entry = self._resolve_meeting_entry(sub, is_mine)
        if entry is None:
            return None
        if sub[3] not in self._store.list_files(entry.meeting.id):
            return None
        return entry.meeting.id, sub[2], sub[3]

    def _resolve_size(self, path: str) -> int | None:
        """Cheap size lookup for stat / readdir / lookup. Never blocks on the network.

        Returns None if `path` is not a known file (caller raises ENOENT).
        Returns 0 if the meeting exists but its content isn't on disk yet —
        the file appears in directory listings but reads return empty until
        the backfill task fetches it.
        """
        if path == f"/{_AUTH_EXPIRED_NAME}":
            return len(_AUTH_EXPIRED_CONTENT) if self._store.is_auth_fatal else None
        if path == f"/{_CHAT_AUTH_EXPIRED_NAME}":
            return len(_CHAT_AUTH_EXPIRED_CONTENT) if self._store.is_chat_auth_fatal else None
        warning_parsed = self._resolve_overlap_warning_file(path)
        if warning_parsed is not None:
            return _OVERLAP_WARNING_SIZE
        ghost_parsed = self._resolve_ghost_file(path)
        if ghost_parsed is not None:
            meeting_id, _slug, filename = ghost_parsed
            return self._store.get_ghost_file_size(meeting_id, filename)
        overlap_parsed = self._resolve_overlap_file(path)
        if overlap_parsed is not None:
            meeting_id, _slug, overlap_dirname, filename = overlap_parsed
            return self._store.get_overlap_file_size(meeting_id, overlap_dirname, filename)
        parsed = self._resolve_meeting_file(path)
        if parsed is None:
            return None
        meeting_id, _slug, filename = parsed
        return self._store.get_file_size(meeting_id, filename)

    def _resolve_content(self, path: str) -> tuple[bytes | None, bool]:
        """Resolve a file path to its rendered content bytes.

        Returns (content, is_completed).

        Slow path: may trigger an API fetch. Only call from `open` (which
        loads bytes for `read` to serve), never from `readdir` / `lookup` /
        `getattr`.
        """
        if path == f"/{_AUTH_EXPIRED_NAME}":
            return (_AUTH_EXPIRED_CONTENT, True) if self._store.is_auth_fatal else (None, False)
        if path == f"/{_CHAT_AUTH_EXPIRED_NAME}":
            return (
                (_CHAT_AUTH_EXPIRED_CONTENT, True) if self._store.is_chat_auth_fatal
                else (None, False)
            )
        warning_parsed = self._resolve_overlap_warning_file(path)
        if warning_parsed is not None:
            meeting_id, _slug = warning_parsed
            return self._store.get_overlap_warning(meeting_id), True
        ghost_parsed = self._resolve_ghost_file(path)
        if ghost_parsed is not None:
            meeting_id, _slug, filename = ghost_parsed
            return self._store.get_ghost_file(meeting_id, filename), True
        overlap_parsed = self._resolve_overlap_file(path)
        if overlap_parsed is not None:
            meeting_id, _slug, overlap_dirname, filename = overlap_parsed
            return self._store.get_overlap_file(meeting_id, overlap_dirname, filename), True
        parsed = self._resolve_meeting_file(path)
        if parsed is None:
            return None, False
        meeting_id, slug, filename = parsed
        try:
            return self._store.get_file(meeting_id, filename)
        except Exception:
            log.exception("Error getting file %s for %s", filename, slug)
            return None, False

    def _get_marker_attr(self, inode: int, path: str) -> pyfuse3.EntryAttributes | None:
        if path not in (f"/{_AUTH_EXPIRED_NAME}", f"/{_CHAT_AUTH_EXPIRED_NAME}"):
            return None
        size = self._resolve_size(path)
        if size is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return _make_file_attr(inode, size)

    def _list_date_subtree(self, sub: list[str], is_mine: bool) -> list[tuple[str, bool]]:
        """List entries for the shared month/day/meeting depth levels."""
        if len(sub) == _SUB_DEPTH_MONTH:
            days = (
                self._store.list_days_mine(sub[0]) if is_mine
                else self._store.list_days(sub[0])
            )
            return [(d, True) for d in days]
        if len(sub) == _SUB_DEPTH_DAY:
            date_str = f"{sub[0]}-{sub[1]}"
            meetings = (
                self._store.list_meetings_mine(date_str) if is_mine
                else self._store.list_meetings(date_str)
            )
            return [(slug, True) for slug in meetings]
        if len(sub) == _SUB_DEPTH_MEETING:
            entry = self._resolve_meeting_entry(sub, is_mine)
            if entry is None:
                return []
            entries = [(f, False) for f in self._store.list_files(entry.meeting.id)]
            if self._store.get_ghost_id(entry.meeting.id) is not None:
                entries.append(("ghost", True))
            if self._store.get_overlap_ids(entry.meeting.id):
                entries.append((_OVERLAP_WARNING_FILE, False))
                entries.extend(
                    (dirname, True)
                    for dirname in self._store.get_overlap_dirnames(entry.meeting.id)
                )
            return entries
        if len(sub) == _SUB_DEPTH_GHOST_DIR and self._resolve_ghost_parent_id(sub, is_mine) is not None:
            return [(f, False) for f in MEETING_FILES]
        if len(sub) == _SUB_DEPTH_OVERLAP_DIR and self._resolve_overlap_parent_id(sub, is_mine) is not None:
            return [(f, False) for f in MEETING_FILES]
        return []

    def _list_dir(self, path: str) -> list[tuple[str, bool]]:
        """List directory entries as (name, is_dir) tuples."""
        is_mine, sub = _parse_path(path)
        try:
            if not is_mine and not sub:
                entries: list[tuple[str, bool]] = [
                    (m, True) for m in self._store.list_year_months()
                ]
                entries.append((_LIVE_DIR, True))
                if self._store.user_email:
                    entries.append((_MINE_DIR, True))
                if self._store.is_chat_auth_fatal:
                    entries.insert(0, (_CHAT_AUTH_EXPIRED_NAME, False))
                if self._store.is_auth_fatal:
                    entries.insert(0, (_AUTH_EXPIRED_NAME, False))
                return entries
            if not is_mine and len(sub) == 1 and sub[0] == _LIVE_DIR:
                return [(name, False) for name in self._store.list_live_dirnames()]
            if is_mine and not sub:
                return [(m, True) for m in self._store.list_year_months_mine()]
            return self._list_date_subtree(sub, is_mine)
        except Exception:
            log.exception("Error listing directory %s", path)
            return []

    def _date_subdir_attr(
        self,
        inode: int,
        path: str,
        sub: list[str],
        is_mine: bool,
    ) -> pyfuse3.EntryAttributes | None:
        if len(sub) < _SUB_DEPTH_FILE:
            return _make_dir_attr(inode, timeout=self._timeout_for_path(path))
        if len(sub) == _SUB_DEPTH_GHOST_DIR and self._resolve_ghost_parent_id(sub, is_mine) is not None:
            return _make_dir_attr(inode, timeout=_STATIC_TIMEOUT)
        if len(sub) == _SUB_DEPTH_OVERLAP_DIR and self._resolve_overlap_parent_id(sub, is_mine) is not None:
            return _make_dir_attr(inode, timeout=_STATIC_TIMEOUT)
        return None

    async def getattr(
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        path = self._get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        marker_attr = self._get_marker_attr(inode, path)
        if marker_attr is not None:
            return marker_attr

        is_mine, sub = _parse_path(path)

        # /live/<meeting-id> — symlink
        if not is_mine and len(sub) == 2 and sub[0] == _LIVE_DIR:
            target = self._store.get_live_symlink_target(sub[1])
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            tgt = target.encode()
            self._remember_symlink(inode, tgt)
            return _make_symlink_attr(inode, len(tgt), timeout=self._timeout_for_path(path))

        # /mine/ requires user_email to be configured
        if is_mine and not sub and not self._store.user_email:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # Directories: root, live, mine root, month, day, meeting
        subdir_attr = self._date_subdir_attr(inode, path, sub, is_mine)
        if subdir_attr is not None:
            return subdir_attr

        # Files — cheap stat-only path (no API calls).
        if len(sub) in {_SUB_DEPTH_FILE, _SUB_DEPTH_GHOST_FILE, _SUB_DEPTH_OVERLAP_FILE}:
            size = self._resolve_size(path)
            if size is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            name = path.rsplit("/", 1)[-1]
            return _make_file_attr(
                inode,
                size,
                executable=name in _EXECUTABLE_NAMES,
                timeout=self._timeout_for_path(path),
            )

        raise pyfuse3.FUSEError(errno.ENOENT)

    async def lookup(
        self,
        parent_inode: int,
        name: bytes,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        parent_path = self._get_path(parent_inode)
        if parent_path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        child_name = name.decode("utf-8", errors="surrogateescape")
        child_path = f"/{child_name}" if parent_path == "/" else f"{parent_path}/{child_name}"

        entries = self._list_dir(parent_path)
        found = False
        is_dir = False
        for entry_name, entry_is_dir in entries:
            if entry_name == child_name:
                found = True
                is_dir = entry_is_dir
                break

        if not found:
            raise pyfuse3.FUSEError(errno.ENOENT)

        inode = self._get_or_create_inode(child_path)

        if is_dir:
            return _make_dir_attr(inode, timeout=self._timeout_for_path(child_path))

        # /live/<meeting-id> entries are symlinks
        if parent_path == f"/{_LIVE_DIR}":
            target = self._store.get_live_symlink_target(child_name)
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            tgt = target.encode()
            self._remember_symlink(inode, tgt)
            return _make_symlink_attr(inode, len(tgt), timeout=self._timeout_for_path(child_path))

        # Cheap stat-only — no API calls. `open` does the actual fetch.
        size = self._resolve_size(child_path)
        if size is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return _make_file_attr(
            inode,
            size,
            executable=child_name in _EXECUTABLE_NAMES,
            timeout=self._timeout_for_path(child_path),
        )

    async def opendir(
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.FileHandleT:
        path = self._get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return pyfuse3.FileHandleT(inode)

    async def readdir(
        self,
        fh: int,
        start_id: int,
        token: pyfuse3.ReaddirToken,
    ) -> None:
        path = self._get_path(fh)
        if path is None:
            return

        entries = self._list_dir(path)

        for idx, (name, is_dir) in enumerate(entries):
            if idx < start_id:
                continue

            child_path = f"/{name}" if path == "/" else f"{path}/{name}"
            child_inode = self._get_or_create_inode(child_path)

            if is_dir:
                attr = _make_dir_attr(child_inode, timeout=self._timeout_for_path(child_path))
            elif path == f"/{_LIVE_DIR}":
                target = self._store.get_live_symlink_target(name)
                if target is None:
                    # Meeting vanished between list and readdir — skip
                    continue
                tgt = target.encode()
                self._remember_symlink(child_inode, tgt)
                attr = _make_symlink_attr(
                    child_inode,
                    len(tgt),
                    timeout=self._timeout_for_path(child_path),
                )
            else:
                # Cheap stat-only — `open` will do the actual fetch on read.
                size = self._resolve_size(child_path) or 0
                attr = _make_file_attr(
                    child_inode,
                    size,
                    executable=name in _EXECUTABLE_NAMES,
                    timeout=self._timeout_for_path(child_path),
                )

            if not pyfuse3.readdir_reply(token, name.encode("utf-8"), attr, idx + 1):
                break

    async def open(
        self,
        inode: int,
        flags: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.FileInfo:
        # Read-only filesystem: reject any write-mode opens
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_TRUNC | os.O_CREAT):
            raise pyfuse3.FUSEError(errno.EROFS)

        path = self._get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # Slow path: may trigger an API fetch. Wrapped in to_thread so the
        # trio event loop isn't blocked during a 30s HTTP call.
        content, is_completed = await trio.to_thread.run_sync(self._resolve_content, path)
        if content is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if is_completed:
            self._remember_static_content(inode, content)
        else:
            self._mark_dynamic_handle(inode)

        fi = pyfuse3.FileInfo()
        fi.fh = pyfuse3.FileHandleT(inode)
        # Completed meetings never change — let the kernel cache them.
        # Live/in-progress files get keep_cache=False so the next open
        # after release() re-fetches fresh content.
        fi.keep_cache = is_completed
        # In-progress files are listed before we've fetched their bytes, so
        # getattr/lookup often reported st_size=0. direct_io makes readers use
        # our read() result instead of stopping at that stale zero-length stat.
        fi.direct_io = not is_completed
        return fi

    async def release(self, fh: int) -> None:
        """Free in-memory file content when the last reader closes the file.

        This ensures (a) we don't leak memory on long-lived mounts and
        (b) live/in-progress files get re-fetched on next open rather
        than serving stale bytes forever.
        """
        self._release_handle(fh)

    async def forget(self, inode_list: Sequence[tuple[pyfuse3.InodeT, int]]) -> None:
        """Kernel has dropped its reference to these inodes. Free our mappings."""
        for inode, nlookup in inode_list:
            self._forget_inode(inode, nlookup)

    async def readlink(self, inode: int, ctx: pyfuse3.RequestContext) -> bytes:
        with self._state_lock:
            target = self._symlinks.get(inode)
        if target is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return target

    async def read(self, fh: int, off: int, size: int) -> bytes:
        if self._is_dynamic_handle(fh):
            path = self._get_path(fh)
            if path is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            parsed = self._resolve_meeting_file(path)
            if parsed is None:
                raise pyfuse3.FUSEError(errno.EIO)
            meeting_id, _slug, filename = parsed
            content = await trio.to_thread.run_sync(
                self._store.get_cached_file_content,
                meeting_id,
                filename,
            )
            if content is None:
                raise pyfuse3.FUSEError(errno.EIO)
            return content[off : off + size]

        content = self._get_static_content(fh)
        if content is None:
            raise pyfuse3.FUSEError(errno.EIO)
        return content[off : off + size]

    async def statfs(
        self, ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.StatvfsData:
        stat_info = pyfuse3.StatvfsData()
        stat_info.f_bsize = 4096
        stat_info.f_frsize = 4096
        stat_info.f_blocks = 0
        stat_info.f_bfree = 0
        stat_info.f_bavail = 0
        stat_info.f_files = 0
        stat_info.f_ffree = 0
        stat_info.f_favail = 0
        stat_info.f_namemax = 255
        return stat_info
