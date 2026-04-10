"""Meeting store — fetches from Fireflies API, caches, serves rendered files."""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
from pydantic import ValidationError

from .api import (
    FatalAPIError,
    FirefliesClient,
    JsonObject,
    RateLimitedError,
    TransientAPIError,
)
from .models import Meeting, TranscriptDetail
from .renderer import (
    render_meeting_json,
    render_open_script,
    render_participants,
    render_summary,
    render_transcript,
)
from .slug import slugify
from .status_cache import StatusCache

log = logging.getLogger(__name__)


def _make_slug(meeting: Meeting) -> str:
    return slugify(meeting.title) if meeting.title else meeting.id[:12]


def _with_slug(meeting: Meeting) -> Meeting:
    """Return a copy of `meeting` with `slug` populated from its title."""
    return meeting.model_copy(update={"slug": _make_slug(meeting)})


def _render_files(meeting: Meeting, detail: TranscriptDetail) -> dict[str, bytes]:
    """Render the standard set of meeting files. Pure function — no I/O."""
    return {
        "summary.md": render_summary(meeting, detail).encode(),
        "transcript.md": render_transcript(meeting, detail).encode(),
        "participants.md": render_participants(meeting, detail).encode(),
        "meeting.json": render_meeting_json(meeting, detail).encode(),
        "open.sh": render_open_script(meeting).encode(),
    }


# Files inside each meeting directory
MEETING_FILES: tuple[str, ...] = (
    "summary.md",
    "transcript.md",
    "participants.md",
    "meeting.json",
    "open.sh",
)
_IN_PROGRESS = "_in_progress"
# Sentinel file written into detail/<meeting_id>/ AFTER all the meeting
# files are on disk. _load_detail_from_disk and get_uncached_meeting_ids
# both gate on this so an interrupted write doesn't leave half a meeting
# being served forever.
_COMPLETE_SENTINEL = ".complete"


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write `data` to `path` atomically. Crashes mid-write leave the
    target untouched (a stale .tmp file may remain — that's fine; the
    next write overwrites it)."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


# Detail re-fetch interval for non-completed meetings
_DETAIL_TTL = 60.0  # 1 minute (live transcripts grow in real time)

# Backoff parameters
_BACKOFF_INITIAL = 30.0
_BACKOFF_MAX = 900.0  # 15 minutes
_BACKOFF_JITTER = 0.25  # ±25%


@dataclass
class MeetingEntry:
    """Metadata for a meeting in the store."""

    meeting: Meeting
    slug: str


@dataclass
class _CachedFiles:
    """Cached rendered files for a meeting."""

    files: dict[str, bytes]
    fetched_at: float


@dataclass
class _BackoffState:
    """Tracks exponential backoff for API failures."""

    delay: float = 0.0
    until: float = 0.0
    consecutive_timeouts: int = 0
    fatal: bool = False

    def record_success(self) -> None:
        self.delay = 0.0
        self.until = 0.0
        self.consecutive_timeouts = 0

    def record_failure(self, *, is_timeout: bool = False) -> None:
        if is_timeout:
            self.consecutive_timeouts += 1
        else:
            self.consecutive_timeouts = 0

        if self.delay == 0.0:
            self.delay = _BACKOFF_INITIAL
        else:
            self.delay = min(self.delay * 2, _BACKOFF_MAX)

        jitter = self.delay * _BACKOFF_JITTER * (2 * random.random() - 1)
        self.until = time.monotonic() + self.delay + jitter
        log.warning(
            "API backoff: next retry in %.0fs (consecutive_timeouts=%d)",
            self.delay + jitter,
            self.consecutive_timeouts,
        )

    def record_rate_limit(self, retry_after: float | None) -> None:
        if retry_after and retry_after > 0:
            self.delay = retry_after
        else:
            self.delay = min(max(self.delay * 2, _BACKOFF_INITIAL), _BACKOFF_MAX)
        # Positive-only jitter: never schedule a retry EARLIER than the
        # server-supplied window, otherwise we self-inflict another 429.
        jitter = self.delay * _BACKOFF_JITTER * random.random()
        self.until = time.monotonic() + self.delay + jitter
        self.consecutive_timeouts = 0
        log.warning("Rate limited — backing off %.0fs", self.delay + jitter)

    def record_fatal(self) -> None:
        self.fatal = True
        log.error("Fatal API error (401/403) — stopping all retries")

    @property
    def is_backed_off(self) -> bool:
        if self.fatal:
            return True
        return time.monotonic() < self.until


class MeetingStore:
    """Fetches from Fireflies API, caches, and serves meeting file data.

    - Meeting list: TTL-based cache (default 30 min, ±30% jitter)
    - Meeting detail (transcript + summary): cached indefinitely for completed meetings
    - Live meetings: re-fetched every 60s to pick up new sentences
    - Lazy fetching: detail only on first file access
    - All API errors are caught — returns stale/empty data on failure
    """

    def __init__(
        self,
        client: FirefliesClient,
        *,
        list_ttl: float = 1800.0,
        status_cache: StatusCache | None = None,
        user_email: str | None = None,
    ) -> None:
        self._client = client
        self._entries: dict[str, MeetingEntry] = {}  # meeting_id -> entry
        self._file_cache: dict[str, _CachedFiles] = {}  # meeting_id -> cached files
        self._list_cache_time: float = 0.0
        self._list_ttl = list_ttl
        self._current_ttl = list_ttl
        self._backoff = _BackoffState()
        self._status_cache = status_cache or StatusCache()
        self.user_email: str | None = user_email
        self._list_cache_file: Path = self._status_cache.cache_dir / "list.json"
        self._detail_cache_dir: Path = self._status_cache.cache_dir / "detail"
        # Guards mutations to _entries / _file_cache / _list_cache_time
        # / _backoff and snapshot reads of those dicts. Held only for
        # short critical sections — never around API calls — so a slow
        # backfill never blocks foreground readdir/lookup ops.
        self._lock = threading.Lock()
        self._load_list_cache()

    # === Disk persistence ===

    def _save_list_cache(self) -> None:
        try:
            data: dict[str, object] = {
                "v": 1,
                "fetched_at": self._list_cache_time,
                "meetings": [e.meeting.model_dump() for e in self._entries.values()],
            }
            self._list_cache_file.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write_bytes(self._list_cache_file, json.dumps(data).encode())
        except OSError:
            log.warning("Failed to save list cache to disk")

    def _load_list_cache(self) -> None:
        try:
            if not self._list_cache_file.exists():
                return
            data: JsonObject = json.loads(self._list_cache_file.read_text())
            if data.get("v") != 1:
                return
            raw_meetings = data.get("meetings")
            if not isinstance(raw_meetings, list):
                return
            entries: dict[str, MeetingEntry] = {}
            for m in raw_meetings:
                if not isinstance(m, dict):
                    continue
                try:
                    meeting = Meeting.model_validate(m)
                except ValidationError as e:
                    log.warning("Skipping malformed cached meeting: %s", e)
                    continue
                entries[meeting.id] = MeetingEntry(meeting=meeting, slug=meeting.slug)
            self._entries = entries
            fetched_at = data.get("fetched_at")
            self._list_cache_time = float(fetched_at) if isinstance(fetched_at, (int, float)) else 0.0
            self._current_ttl = self._list_ttl * (0.7 + random.random() * 0.6)
            log.info(
                "Loaded %d meetings from disk cache (fetched %.0fs ago)",
                len(entries),
                time.time() - self._list_cache_time,
            )
        except (OSError, json.JSONDecodeError):
            log.warning("Failed to load list cache from disk")

    def _save_detail_to_disk(self, meeting_id: str, files: dict[str, bytes]) -> None:
        try:
            detail_dir = self._detail_cache_dir / meeting_id
            detail_dir.mkdir(parents=True, exist_ok=True)
            # Drop any existing sentinel before rewriting — if the writes
            # below crash, the partially-rewritten dir won't look complete.
            sentinel = detail_dir / _COMPLETE_SENTINEL
            if sentinel.exists():
                sentinel.unlink()
            for filename, content in files.items():
                _atomic_write_bytes(detail_dir / filename, content)
            sentinel.touch()
        except OSError:
            log.warning("Failed to save detail cache for %s", meeting_id)

    def _load_detail_from_disk(self, meeting_id: str) -> _CachedFiles | None:
        try:
            detail_dir = self._detail_cache_dir / meeting_id
            if not detail_dir.is_dir():
                return None
            if not (detail_dir / _COMPLETE_SENTINEL).exists():
                return None
            files = {
                p.name: p.read_bytes()
                for p in detail_dir.iterdir()
                if p.is_file() and p.name != _COMPLETE_SENTINEL
            }
            return _CachedFiles(files=files, fetched_at=time.monotonic()) if files else None
        except OSError:
            log.warning("Failed to load detail cache for %s", meeting_id)
            return None

    def _refresh_if_stale(self) -> None:
        with self._lock:
            now = time.time()
            if now - self._list_cache_time < self._current_ttl:
                return
            if self._backoff.is_backed_off:
                log.debug("Skipping list refresh — in backoff period")
                return
            is_initial = len(self._entries) == 0
        self._fetch_meetings(is_initial=is_initial)

    def _fetch_meetings(self, *, is_initial: bool) -> None:
        """Fetch meeting list from API and populate entries.

        First fetch: all pages. Subsequent refreshes: page 1 only, merged
        with existing entries so older meetings are preserved.
        """
        max_pages = None if is_initial else 1
        label = "all pages" if is_initial else "page 1"
        log.info("Fetching meeting list from Fireflies API (%s)", label)

        # API call — no lock held
        try:
            meetings = self._client.list_transcripts(max_pages=max_pages)
        except RateLimitedError as e:
            with self._lock:
                self._backoff.record_rate_limit(e.retry_after)
            return
        except FatalAPIError:
            with self._lock:
                self._backoff.record_fatal()
            return
        except httpx.TimeoutException:
            log.warning("Timeout fetching meeting list")
            with self._lock:
                self._backoff.record_failure(is_timeout=True)
            return
        except httpx.HTTPError as e:
            log.warning("HTTP error fetching meeting list: %s", e)
            with self._lock:
                self._backoff.record_failure()
            return

        # Build new entries outside the lock (pure computation)
        new_entries: dict[str, MeetingEntry] = {}
        for raw_meeting in meetings:
            meeting = _with_slug(raw_meeting)
            new_entries[meeting.id] = MeetingEntry(meeting=meeting, slug=meeting.slug)

        # Apply under the lock
        with self._lock:
            if is_initial:
                self._entries = new_entries
            else:
                self._entries.update(new_entries)

            self._list_cache_time = time.time()
            jitter_factor = 0.7 + random.random() * 0.6  # [0.7, 1.3]
            self._current_ttl = self._list_ttl * jitter_factor
            self._backoff.record_success()
            log.info(
                "Loaded %d meetings (next refresh in %.0fs)",
                len(self._entries),
                self._current_ttl,
            )
        self._save_list_cache()

    def _resolve_collisions(self, entries: list[MeetingEntry]) -> dict[str, MeetingEntry]:
        """Assign unique directory names, appending -2, -3 for collisions."""
        sorted_entries = sorted(entries, key=lambda e: e.meeting.date_epoch_ms)
        result: dict[str, MeetingEntry] = {}
        slug_count: dict[str, int] = {}

        for entry in sorted_entries:
            base_slug = entry.slug
            count = slug_count.get(base_slug, 0)
            slug_count[base_slug] = count + 1
            dirname = base_slug if count == 0 else f"{base_slug}-{count + 1}"
            result[dirname] = entry

        return result

    def _is_cache_fresh(self, meeting_id: str, cached: _CachedFiles) -> bool:
        """Return True if cached data is still valid and should be returned as-is."""
        if self._status_cache.is_completed(meeting_id):
            return True
        return time.monotonic() - cached.fetched_at < _DETAIL_TTL

    def _fetch_detail(self, meeting: Meeting) -> _CachedFiles | None:
        """Call the API and render files for a meeting. Updates backoff state.

        The API call runs WITHOUT self._lock held (called outside the lock by
        _ensure_files / backfill_one). Backoff state updates are done under
        the lock.
        """
        meeting_id = meeting.id
        try:
            detail = self._client.get_transcript(meeting_id)
        except RateLimitedError as e:
            with self._lock:
                self._backoff.record_rate_limit(e.retry_after)
            return None
        except FatalAPIError:
            with self._lock:
                self._backoff.record_fatal()
            return None
        except TransientAPIError as e:
            log.warning("Transient API error fetching detail for %s: %s", meeting_id, e)
            with self._lock:
                self._backoff.record_failure()
            return None
        except httpx.TimeoutException:
            log.warning("Timeout fetching detail for %s", meeting_id)
            with self._lock:
                self._backoff.record_failure(is_timeout=True)
            return None
        except httpx.HTTPError as e:
            log.warning("HTTP error fetching detail for %s: %s", meeting_id, e)
            with self._lock:
                self._backoff.record_failure()
            return None

        with self._lock:
            self._backoff.record_success()

        # The detail's meeting object came from a separate API call; carry over
        # the slug and date that the list-side computed for this entry so the
        # rendered files match the directory layout.
        detail = detail.model_copy(update={
            "meeting": detail.meeting.model_copy(update={
                "slug": meeting.slug,
                "date_str": meeting.date_str,
            }),
        })

        files = _render_files(detail.meeting, detail)
        if detail.meeting.is_completed:
            self._status_cache.mark_completed(meeting_id)
            self._save_detail_to_disk(meeting_id, files)
        else:
            status_text = (
                f"is_live: {detail.meeting.is_live}\n"
                f"summary_status: {detail.meeting.meeting_info.summary_status}\n"
            )
            files[_IN_PROGRESS] = status_text.encode()

        return _CachedFiles(files=files, fetched_at=time.monotonic())

    def _ensure_files(self, meeting_id: str) -> _CachedFiles | None:
        """Ensure meeting files are fetched and rendered.

        Returns None only on API error or unknown meeting.
        Completed meetings are served from disk after first fetch.
        Live/in-progress meetings expire after _DETAIL_TTL seconds.

        Lock is held only for the cache check / cache update — the slow
        API call inside _fetch_detail runs without the lock.
        """
        # Fast path: cache hit (under lock for a consistent snapshot)
        with self._lock:
            cached = self._file_cache.get(meeting_id)
            if cached is not None and self._is_cache_fresh(meeting_id, cached):
                return cached

            if cached is not None:
                log.info("Detail TTL expired for in-progress meeting %s, re-fetching", meeting_id)
                del self._file_cache[meeting_id]

            entry = self._entries.get(meeting_id)
            if entry is None:
                return None

            # Completed meetings: serve from disk, no API call needed
            if self._status_cache.is_completed(meeting_id):
                disk = self._load_detail_from_disk(meeting_id)
                if disk is not None:
                    self._file_cache[meeting_id] = disk
                    return disk

            if self._backoff.is_backed_off:
                log.debug("Skipping detail fetch for %s — in backoff period", meeting_id)
                return cached

            meeting = entry.meeting  # snapshot for the API call outside lock

        # Slow path: API call without the lock
        log.info("Fetching detail for meeting %s: %s", meeting_id, meeting.title)
        new_cached = self._fetch_detail(meeting)
        if new_cached is not None:
            with self._lock:
                self._file_cache[meeting_id] = new_cached
        return new_cached

    # === Public API ===

    def list_year_months(self) -> list[str]:
        """Return sorted list of YYYY-MM strings, newest first."""
        self._refresh_if_stale()
        with self._lock:
            snapshot = list(self._entries.values())
        months = {e.meeting.date_str[:7] for e in snapshot if e.meeting.date_str}
        return sorted(months, reverse=True)

    def list_days(self, year_month: str) -> list[str]:
        """Return sorted list of DD strings for a given YYYY-MM, newest first."""
        self._refresh_if_stale()
        with self._lock:
            snapshot = list(self._entries.values())
        days = {
            e.meeting.date_str[8:10]
            for e in snapshot
            if e.meeting.date_str and e.meeting.date_str[:7] == year_month
        }
        return sorted(days, reverse=True)

    def list_meetings(self, date_str: str) -> dict[str, MeetingEntry]:
        """Return dirname -> MeetingEntry for a YYYY-MM-DD date, with collision handling."""
        self._refresh_if_stale()
        with self._lock:
            entries = [e for e in self._entries.values() if e.meeting.date_str == date_str]
        return self._resolve_collisions(entries)

    def list_year_months_mine(self) -> list[str]:
        """Return sorted YYYY-MM strings for months containing meetings user organized."""
        self._refresh_if_stale()
        if not self.user_email:
            return []
        with self._lock:
            snapshot = list(self._entries.values())
        months = {
            e.meeting.date_str[:7]
            for e in snapshot
            if e.meeting.date_str and e.meeting.organizer_email == self.user_email
        }
        return sorted(months, reverse=True)

    def list_days_mine(self, year_month: str) -> list[str]:
        """Return sorted DD strings for days in YYYY-MM where user organized a meeting."""
        self._refresh_if_stale()
        if not self.user_email:
            return []
        with self._lock:
            snapshot = list(self._entries.values())
        days = {
            e.meeting.date_str[8:10]
            for e in snapshot
            if e.meeting.date_str
            and e.meeting.date_str[:7] == year_month
            and e.meeting.organizer_email == self.user_email
        }
        return sorted(days, reverse=True)

    def list_meetings_mine(self, date_str: str) -> dict[str, MeetingEntry]:
        """Return dirname -> MeetingEntry for a date, filtered to meetings user organized."""
        self._refresh_if_stale()
        if not self.user_email:
            return {}
        with self._lock:
            entries = [
                e for e in self._entries.values()
                if e.meeting.date_str == date_str and e.meeting.organizer_email == self.user_email
            ]
        return self._resolve_collisions(entries)

    def list_live_meeting_ids(self) -> list[str]:
        """Return IDs of currently live meetings."""
        self._refresh_if_stale()
        with self._lock:
            return [mid for mid, e in self._entries.items() if e.meeting.is_live]

    def get_uncached_meeting_ids(self) -> list[str]:
        """Return IDs of completed meetings that have no disk detail cache yet.

        Gates on the .complete sentinel so a half-populated dir from an
        interrupted write gets re-fetched instead of being treated as cached.
        """
        with self._lock:
            snapshot = list(self._entries.items())
        return [
            mid for mid, e in snapshot
            if e.meeting.is_completed
            and not (self._detail_cache_dir / mid / _COMPLETE_SENTINEL).exists()
        ]

    def backfill_one(self, meeting_id: str) -> None:
        """Fetch, render, and persist one meeting to disk.

        Safe to call from a background thread — touches only disk and the API client.
        Raises RateLimitedError, FatalAPIError, or TransientAPIError on API errors;
        caller handles backoff. No-ops if already cached or entry not found.
        """
        if (self._detail_cache_dir / meeting_id / _COMPLETE_SENTINEL).exists():
            return
        with self._lock:
            entry = self._entries.get(meeting_id)
        if entry is None:
            return
        # API call outside lock; caller (get_uncached_meeting_ids) already
        # verified the meeting is_completed so marking it as such is correct.
        raw_detail = self._client.get_transcript(meeting_id)
        detail = raw_detail.model_copy(update={
            "meeting": raw_detail.meeting.model_copy(update={
                "slug": entry.meeting.slug,
                "date_str": entry.meeting.date_str,
            }),
        })
        files = _render_files(detail.meeting, detail)
        self._save_detail_to_disk(meeting_id, files)
        self._status_cache.mark_completed(meeting_id)

    def get_live_symlink_target(self, meeting_id: str) -> str | None:
        """Return the relative symlink target for a live meeting from the /live/ directory.

        Target is of the form ../YYYY-MM/DD/<dirname> where dirname is the
        collision-resolved name in the full date tree.
        """
        entry = self._entries.get(meeting_id)
        if entry is None or not entry.meeting.is_live:
            return None
        date_str = entry.meeting.date_str
        year_month, day = date_str[:7], date_str[8:10]
        for dirname, e in self.list_meetings(date_str).items():
            if e.meeting.id == meeting_id:
                return f"../{year_month}/{day}/{dirname}"
        return None

    def get_file(self, meeting_id: str, filename: str) -> tuple[bytes | None, bool]:
        """Return (file_bytes, is_completed) for a meeting.

        Slow path: may trigger an API fetch if the meeting isn't on disk and
        isn't in the in-memory cache. Only call from `open` / `read`, never
        from `readdir` / `lookup` / `getattr`.

        Returns (None, False) on failure.
        """
        cached = self._ensure_files(meeting_id)
        if cached is None:
            return None, False
        completed = self._status_cache.is_completed(meeting_id)
        return cached.files.get(filename), completed

    def get_file_size(self, meeting_id: str, filename: str) -> int:
        """Cheap stat-only size lookup. Never blocks on the network.

        Checks the in-memory cache (live/in-progress meetings) first, then
        stats the disk cache file. Returns 0 if neither has it — the file
        will still appear in directory listings but will look empty until
        the backfill task fetches it. Used by `readdir` / `lookup` / `getattr`
        so that listing a directory never blocks on an API call.
        """
        with self._lock:
            cached = self._file_cache.get(meeting_id)
            if cached is not None:
                data = cached.files.get(filename)
                if data is not None:
                    return len(data)
        try:
            return (self._detail_cache_dir / meeting_id / filename).stat().st_size
        except OSError:
            return 0

    def list_files(self, meeting_id: str) -> list[str]:
        """Return filenames available for a meeting.

        Includes MEETING_FILES plus _in_progress if the meeting is not
        yet fully processed. Does NOT trigger a detail fetch.
        """
        if self._status_cache.is_completed(meeting_id):
            return list(MEETING_FILES)
        entry = self._entries.get(meeting_id)
        if entry is not None and entry.meeting.is_completed:
            return list(MEETING_FILES)
        return [*MEETING_FILES, _IN_PROGRESS]

    def invalidate(self) -> None:
        """Force re-fetch of meeting list on next access."""
        with self._lock:
            self._list_cache_time = 0.0

    @property
    def is_auth_fatal(self) -> bool:
        """True if a fatal 401/403 has stopped all retries."""
        return self._backoff.fatal

    def force_refresh(self) -> None:
        """Full cache invalidation: list, non-completed details, backoff."""
        with self._lock:
            self._list_cache_time = 0.0
            self._backoff = _BackoffState()
            to_evict = [
                mid for mid in self._file_cache
                if not self._status_cache.is_completed(mid)
            ]
            for mid in to_evict:
                del self._file_cache[mid]
        log.info(
            "Force refresh: cleared list cache, backoff, and %d non-completed file caches",
            len(to_evict),
        )
