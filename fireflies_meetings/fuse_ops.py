"""pyfuse3 Operations subclass for the Fireflies meetings filesystem."""

from __future__ import annotations

import errno
import logging
import os
import stat
import time

import pyfuse3

from .inode_map import InodeMap
from .store import MeetingStore

log = logging.getLogger(__name__)

# Path structure (non-mine):
#   depth 0  /
#   depth 1  /2026-03/
#   depth 2  /2026-03/25/
#   depth 3  /2026-03/25/backend-q1-retrospective/
#   depth 4  /2026-03/25/backend-q1-retrospective/summary.md
#
# mine/ subtree adds one level at the front:
#   depth 1  /mine/
#   depth 2  /mine/2026-03/
#   ...depth N+1 mirrors depth N above

_MINE_DIR = "mine"
_LIVE_DIR = "live"
_EXECUTABLE_NAMES = frozenset({"open.sh"})
_AUTH_EXPIRED_NAME = "AUTHENTICATION_EXPIRED"
_AUTH_EXPIRED_CONTENT = b"""\
Authentication has expired. Check that your API key is valid and run:

    fireflies-meetings mount --api-key ~/.config/fireflies-meetings/api_key

Then restart the service.
"""

# sub_depth = number of path components after stripping the optional "mine/" prefix.
# Directories have sub_depth 0-3; files have sub_depth 4.
_SUB_DEPTH_MONTH = 1
_SUB_DEPTH_DAY = 2
_SUB_DEPTH_MEETING = 3
_SUB_DEPTH_FILE = 4


def _now_ns() -> int:
    return int(time.time() * 1e9)


def _make_dir_attr(inode: int) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = pyfuse3.InodeT(inode)
    entry.st_mode = stat.S_IFDIR | 0o555
    entry.st_nlink = 2
    entry.st_size = 0
    now = _now_ns()
    entry.st_atime_ns = now
    entry.st_mtime_ns = now
    entry.st_ctime_ns = now
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _make_symlink_attr(inode: int, target_len: int) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = pyfuse3.InodeT(inode)
    entry.st_mode = stat.S_IFLNK | 0o777
    entry.st_nlink = 1
    entry.st_size = target_len
    now = _now_ns()
    entry.st_atime_ns = now
    entry.st_mtime_ns = now
    entry.st_ctime_ns = now
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _make_file_attr(
    inode: int, size: int, *, executable: bool = False,
) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = pyfuse3.InodeT(inode)
    entry.st_mode = stat.S_IFREG | (0o555 if executable else 0o444)
    entry.st_nlink = 1
    entry.st_size = size
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


class FirefliesMeetingOps(pyfuse3.Operations):
    """Read-only FUSE operations for Fireflies.ai meetings."""

    def __init__(self, store: MeetingStore) -> None:
        super().__init__()
        self._store = store
        self._inodes = InodeMap()
        self._content: dict[int, bytes] = {}
        self._symlinks: dict[int, bytes] = {}  # inode -> symlink target bytes

    def _resolve_meeting_file(self, path: str) -> tuple[str, str, str] | None:
        """Parse a path into (meeting_id, slug, filename) if it's a meeting file.

        Returns None for non-file paths or paths that don't match a known meeting.
        Cheap: only consults the in-memory `_store.list_meetings*` snapshots.
        """
        is_mine, sub = _parse_path(path)
        if len(sub) != _SUB_DEPTH_FILE:
            return None

        year_month, day, slug, filename = sub
        date_str = f"{year_month}-{day}"
        try:
            meetings = (
                self._store.list_meetings_mine(date_str) if is_mine
                else self._store.list_meetings(date_str)
            )
        except Exception:
            log.exception("Error listing meetings for %s", date_str)
            return None
        entry = meetings.get(slug)
        if entry is None:
            return None
        return entry.meeting.id, slug, filename

    def _resolve_size(self, path: str) -> int | None:
        """Cheap size lookup for stat / readdir / lookup. Never blocks on the network.

        Returns None if `path` is not a known file (caller raises ENOENT).
        Returns 0 if the meeting exists but its content isn't on disk yet —
        the file appears in directory listings but reads return empty until
        the backfill task fetches it.
        """
        if path == f"/{_AUTH_EXPIRED_NAME}":
            return len(_AUTH_EXPIRED_CONTENT) if self._store.is_auth_fatal else None
        parsed = self._resolve_meeting_file(path)
        if parsed is None:
            return None
        meeting_id, _slug, filename = parsed
        return self._store.get_file_size(meeting_id, filename)

    def _resolve_content(self, path: str) -> bytes | None:
        """Resolve a file path to its rendered content bytes.

        Slow path: may trigger an API fetch. Only call from `open` (which
        loads bytes for `read` to serve), never from `readdir` / `lookup` /
        `getattr`.
        """
        if path == f"/{_AUTH_EXPIRED_NAME}":
            return _AUTH_EXPIRED_CONTENT if self._store.is_auth_fatal else None
        parsed = self._resolve_meeting_file(path)
        if parsed is None:
            return None
        meeting_id, slug, filename = parsed
        try:
            return self._store.get_file(meeting_id, filename)
        except Exception:
            log.exception("Error getting file %s for %s", filename, slug)
            return None

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
            date_str = f"{sub[0]}-{sub[1]}"
            entry = (
                self._store.list_meetings_mine(date_str) if is_mine
                else self._store.list_meetings(date_str)
            ).get(sub[2])
            if entry is None:
                return []
            return [(f, False) for f in self._store.list_files(entry.meeting.id)]
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
                if self._store.is_auth_fatal:
                    entries.insert(0, (_AUTH_EXPIRED_NAME, False))
                return entries
            if not is_mine and len(sub) == 1 and sub[0] == _LIVE_DIR:
                return [(mid, False) for mid in self._store.list_live_meeting_ids()]
            if is_mine and not sub:
                return [(m, True) for m in self._store.list_year_months_mine()]
            return self._list_date_subtree(sub, is_mine)
        except Exception:
            log.exception("Error listing directory %s", path)
            return []

    async def getattr(
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if path == f"/{_AUTH_EXPIRED_NAME}":
            size = self._resolve_size(path)
            if size is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            return _make_file_attr(inode, size)

        is_mine, sub = _parse_path(path)

        # /live/<meeting-id> — symlink
        if not is_mine and len(sub) == 2 and sub[0] == _LIVE_DIR:
            target = self._store.get_live_symlink_target(sub[1])
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            tgt = target.encode()
            self._symlinks[inode] = tgt
            return _make_symlink_attr(inode, len(tgt))

        # /mine/ requires user_email to be configured
        if is_mine and not sub and not self._store.user_email:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # Directories: root, live, mine root, month, day, meeting
        if len(sub) < _SUB_DEPTH_FILE:
            return _make_dir_attr(inode)

        # Files — cheap stat-only path (no API calls).
        if len(sub) == _SUB_DEPTH_FILE:
            size = self._resolve_size(path)
            if size is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            name = path.rsplit("/", 1)[-1]
            return _make_file_attr(inode, size, executable=name in _EXECUTABLE_NAMES)

        raise pyfuse3.FUSEError(errno.ENOENT)

    async def lookup(
        self,
        parent_inode: int,
        name: bytes,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        parent_path = self._inodes.get_path(parent_inode)
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

        inode = self._inodes.get_or_create(child_path)

        if is_dir:
            return _make_dir_attr(inode)

        # /live/<meeting-id> entries are symlinks
        if parent_path == f"/{_LIVE_DIR}":
            target = self._store.get_live_symlink_target(child_name)
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            tgt = target.encode()
            self._symlinks[inode] = tgt
            return _make_symlink_attr(inode, len(tgt))

        # Cheap stat-only — no API calls. `open` does the actual fetch.
        size = self._resolve_size(child_path)
        if size is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return _make_file_attr(inode, size, executable=child_name in _EXECUTABLE_NAMES)

    async def opendir(
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.FileHandleT:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return pyfuse3.FileHandleT(inode)

    async def readdir(
        self,
        fh: int,
        start_id: int,
        token: pyfuse3.ReaddirToken,
    ) -> None:
        path = self._inodes.get_path(fh)
        if path is None:
            return

        entries = self._list_dir(path)

        for idx, (name, is_dir) in enumerate(entries):
            if idx < start_id:
                continue

            child_path = f"/{name}" if path == "/" else f"{path}/{name}"
            child_inode = self._inodes.get_or_create(child_path)

            if is_dir:
                attr = _make_dir_attr(child_inode)
            elif path == f"/{_LIVE_DIR}":
                target = self._store.get_live_symlink_target(name)
                tgt = target.encode() if target else b""
                self._symlinks[child_inode] = tgt
                attr = _make_symlink_attr(child_inode, len(tgt))
            else:
                # Cheap stat-only — `open` will do the actual fetch on read.
                size = self._resolve_size(child_path) or 0
                attr = _make_file_attr(child_inode, size, executable=name in _EXECUTABLE_NAMES)

            if not pyfuse3.readdir_reply(token, name.encode("utf-8"), attr, idx + 1):
                break

    async def open(
        self,
        inode: int,
        flags: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.FileInfo:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if inode not in self._content:
            content = self._resolve_content(path)
            if content is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            self._content[inode] = content

        fi = pyfuse3.FileInfo()
        fi.fh = pyfuse3.FileHandleT(inode)
        fi.keep_cache = True
        return fi

    async def readlink(self, inode: int, ctx: pyfuse3.RequestContext) -> bytes:
        target = self._symlinks.get(inode)
        if target is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return target

    async def read(self, fh: int, off: int, size: int) -> bytes:
        content = self._content.get(fh)
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
        stat_info.f_files = self._inodes.count
        stat_info.f_ffree = 0
        stat_info.f_favail = 0
        stat_info.f_namemax = 255
        return stat_info
