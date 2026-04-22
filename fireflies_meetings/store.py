"""Meeting store — fetches from Fireflies API, caches, serves rendered files."""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx
from pydantic import ValidationError

from .api import (
    FatalAPIError,
    FirefliesClient,
    JsonObject,
    RateLimitedError,
    TranscriptNotFoundError,
    TransientAPIError,
)
from .models import Meeting, Sentence, TranscriptDetail
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


def _make_stub_detail(meeting: Meeting) -> tuple[Meeting, TranscriptDetail]:
    """Create a stub TranscriptDetail for a meeting whose transcript is gone from Fireflies.

    Returns (updated_meeting, stub_detail). The meeting's summary_status is set to
    "missing_from_api" so renderers show an appropriate message instead of "not yet available".
    """
    stub_meeting = meeting.model_copy(update={
        "meeting_info": meeting.meeting_info.model_copy(update={"summary_status": "missing_from_api"}),
    })
    return stub_meeting, TranscriptDetail(meeting=stub_meeting)


def _can_persist_detail(detail: TranscriptDetail) -> bool:
    """Return True when a detail fetch is terminal and complete enough to cache forever."""
    return detail.meeting.is_completed and not detail.transcript_error


def _render_in_progress_status(detail: TranscriptDetail) -> bytes:
    status_text = (
        f"is_live: {detail.meeting.is_live}\n"
        f"summary_status: {detail.meeting.meeting_info.summary_status}\n"
    )
    if detail.transcript_error:
        status_text += f"transcript_error: {detail.transcript_error}\n"
    return status_text.encode()


def _sort_sentences(sentences: list[Sentence]) -> list[Sentence]:
    return sorted(sentences, key=lambda sentence: (sentence.start_time, sentence.index))


def _merge_sentences(
    base_sentences: list[Sentence],
    live_rows: dict[str, Sentence] | None,
) -> list[Sentence]:
    if not live_rows:
        return _sort_sentences(list(base_sentences))

    merged = {str(sentence.index): sentence for sentence in base_sentences}
    merged.update(live_rows)
    return _sort_sentences(list(merged.values()))


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
        self._live_details: dict[str, TranscriptDetail] = {}
        self._live_transcript_rows: dict[str, dict[str, Sentence]] = {}
        self._list_cache_time: float = 0.0
        self._list_ttl = list_ttl
        self._current_ttl = list_ttl
        self._backoff = _BackoffState()
        self._status_cache = status_cache or StatusCache()
        self.user_email: str | None = user_email
        self._list_cache_file: Path = self._status_cache.cache_dir / "list.json"
        self._detail_cache_dir: Path = self._status_cache.cache_dir / "detail"
        self._live_change_callback: Callable[[str], None] | None = None
        self._chat_auth_fatal: bool = False
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
                for mid, new_entry in new_entries.items():
                    existing = self._entries.get(mid)
                    if (
                        existing is not None
                        and existing.meeting.is_live
                        and not new_entry.meeting.is_live
                        and not new_entry.meeting.summary_is_terminal
                    ):
                        # List API doesn't reflect live state reliably (is_live
                        # is False even for in-progress meetings). watch_meeting
                        # and active_meetings poll are the positive sources;
                        # list refresh only flips live→done when summary_status
                        # goes terminal.
                        preserved = new_entry.meeting.model_copy(update={"is_live": True})
                        self._entries[mid] = MeetingEntry(
                            meeting=preserved, slug=new_entry.slug,
                        )
                    else:
                        self._entries[mid] = new_entry

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

    def _fetch_detail(self, meeting: Meeting) -> tuple[_CachedFiles | None, bool]:
        """Call the API and render files for a meeting. Updates backoff state.

        The API call runs WITHOUT self._lock held (called outside the lock by
        _ensure_files / backfill_one). Backoff state updates are done under
        the lock.
        """
        meeting_id = meeting.id
        try:
            detail = self._client.get_transcript(meeting_id)
        except TranscriptNotFoundError:
            log.info(
                "Transcript %s no longer available from Fireflies; writing stub",
                meeting_id,
            )
            stub_meeting, stub_detail = _make_stub_detail(meeting)
            files = _render_files(stub_meeting, stub_detail)
            self._status_cache.mark_completed(meeting_id)
            self._save_detail_to_disk(meeting_id, files)
            return _CachedFiles(files=files, fetched_at=time.monotonic()), True
        except RateLimitedError as e:
            with self._lock:
                self._backoff.record_rate_limit(e.retry_after)
            return None, False
        except FatalAPIError:
            with self._lock:
                self._backoff.record_fatal()
            return None, False
        except TransientAPIError as e:
            log.warning("Transient API error fetching detail for %s: %s", meeting_id, e)
            with self._lock:
                self._backoff.record_failure()
            return None, False
        except httpx.TimeoutException:
            log.warning("Timeout fetching detail for %s", meeting_id)
            with self._lock:
                self._backoff.record_failure(is_timeout=True)
            return None, False
        except httpx.HTTPError as e:
            log.warning("HTTP error fetching detail for %s: %s", meeting_id, e)
            with self._lock:
                self._backoff.record_failure()
            return None, False

        with self._lock:
            self._backoff.record_success()
            detail, live_state_changed = self._merge_live_stream_state_locked(meeting_id, detail)

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
        if _can_persist_detail(detail):
            self._status_cache.mark_completed(meeting_id)
            self._save_detail_to_disk(meeting_id, files)
        else:
            files[_IN_PROGRESS] = _render_in_progress_status(detail)

        return _CachedFiles(files=files, fetched_at=time.monotonic()), live_state_changed

    def _cache_rendered_detail_locked(self, meeting_id: str, detail: TranscriptDetail) -> None:
        files = _render_files(detail.meeting, detail)
        if not _can_persist_detail(detail):
            files[_IN_PROGRESS] = _render_in_progress_status(detail)
        self._file_cache[meeting_id] = _CachedFiles(files=files, fetched_at=time.monotonic())

    def _overlay_live_rows_locked(self, meeting_id: str, detail: TranscriptDetail) -> TranscriptDetail:
        live_rows = self._live_transcript_rows.get(meeting_id)
        if live_rows is None:
            return detail
        return detail.model_copy(update={
            "sentences": _merge_sentences(detail.sentences, live_rows),
            "transcript_error": "",
        })

    def _merge_live_stream_state_locked(
        self,
        meeting_id: str,
        detail: TranscriptDetail,
    ) -> tuple[TranscriptDetail, bool]:
        live_state_changed = False
        if not detail.meeting.is_live:
            self._live_details.pop(meeting_id, None)
            self._live_transcript_rows.pop(meeting_id, None)
            if detail.meeting.summary_is_terminal:
                # Detail fetch is the authoritative negative signal for
                # Chat-discovered meetings: when summary_status goes terminal
                # the meeting is done, so clear the locally-held is_live flag.
                entry = self._entries.get(meeting_id)
                if entry is not None and entry.meeting.is_live:
                    done = entry.meeting.model_copy(update={"is_live": False})
                    self._entries[meeting_id] = MeetingEntry(
                        meeting=done, slug=entry.slug,
                    )
                    live_state_changed = True
                    log.info(
                        "Detail fetch: marked %s %r as done (summary_status=%s)",
                        meeting_id, done.title,
                        done.meeting_info.summary_status,
                    )
            return detail, live_state_changed

        self._live_details[meeting_id] = detail
        return self._overlay_live_rows_locked(meeting_id, detail), live_state_changed

    def _ensure_files(self, meeting_id: str) -> _CachedFiles | None:
        """Ensure meeting files are fetched and rendered.

        Returns None only on API error or unknown meeting.
        Completed meetings are served from disk after first fetch.
        Live/in-progress meetings expire after _DETAIL_TTL seconds.

        Lock is held only for the cache check / cache update — the slow
        API call inside _fetch_detail runs without the lock.
        """
        # Fast path: cache hit (under lock for a consistent snapshot)
        stale_cached: _CachedFiles | None = None
        with self._lock:
            cached = self._file_cache.get(meeting_id)
            if cached is not None and self._is_cache_fresh(meeting_id, cached):
                return cached

            if cached is not None:
                log.info("Detail TTL expired for in-progress meeting %s, re-fetching", meeting_id)
                stale_cached = cached

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
        new_cached, _ = self._fetch_detail(meeting)
        if new_cached is not None:
            with self._lock:
                self._file_cache[meeting_id] = new_cached
            self._notify_live_change(meeting_id)
            return new_cached
        return stale_cached

    # === Public API ===

    def list_year_months(self) -> list[str]:
        """Return sorted list of YYYY-MM strings, newest first."""
        self._refresh_if_stale()
        with self._lock:
            snapshot = list(self._entries.values())
        months = {e.meeting.date_str[:7] for e in snapshot if e.meeting.date_str}
        return sorted(months, reverse=True)

    def mark_list_cache_fresh(self) -> None:
        """Mark the in-memory list snapshot as freshly fetched."""
        with self._lock:
            self._list_cache_time = time.time()

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

    def list_live_dirnames(self) -> list[str]:
        """Return collision-resolved slug dirnames of currently-live meetings.

        Names are slug-based (e.g. ``backend-roundtable``) so ``/live/`` reads
        like a human-named directory. Collisions within the live subset get
        ``-2``, ``-3`` suffixes via ``_resolve_collisions``.
        """
        self._refresh_if_stale()
        with self._lock:
            live = [e for e in self._entries.values() if e.meeting.is_live]
        return list(self._resolve_collisions(live).keys())

    def _live_entry_by_dirname(self, dirname: str) -> MeetingEntry | None:
        with self._lock:
            live = [e for e in self._entries.values() if e.meeting.is_live]
        return self._resolve_collisions(live).get(dirname)

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

        Permanently-deleted transcripts (404 / object_not_found) are handled
        internally: stub files are written so the meeting stays visible with
        its list metadata, and the backfill loop never retries it.
        """
        if (self._detail_cache_dir / meeting_id / _COMPLETE_SENTINEL).exists():
            return
        with self._lock:
            entry = self._entries.get(meeting_id)
        if entry is None:
            return
        try:
            raw_detail = self._client.get_transcript(meeting_id)
        except TranscriptNotFoundError:
            log.info(
                "Transcript %s no longer available from Fireflies; writing stub",
                meeting_id,
            )
            stub_meeting, stub_detail = _make_stub_detail(entry.meeting)
            files = _render_files(stub_meeting, stub_detail)
            self._save_detail_to_disk(meeting_id, files)
            self._status_cache.mark_completed(meeting_id)
            return
        # API call outside lock; caller (get_uncached_meeting_ids) already
        # verified the meeting is_completed so marking it as such is correct.
        detail = raw_detail.model_copy(update={
            "meeting": raw_detail.meeting.model_copy(update={
                "slug": entry.meeting.slug,
                "date_str": entry.meeting.date_str,
            }),
        })
        if detail.transcript_error:
            raise TransientAPIError(detail.transcript_error)
        files = _render_files(detail.meeting, detail)
        self._save_detail_to_disk(meeting_id, files)
        self._status_cache.mark_completed(meeting_id)

    def _fetch_detail_for_watch(self, meeting_id: str) -> TranscriptDetail | None:
        """Fetch detail for a watch-meeting call, handling backoff book-keeping.

        Returns None on any API error (backoff state is updated accordingly).
        """
        try:
            return self._client.get_transcript(meeting_id)
        except TranscriptNotFoundError:
            log.info("watch_meeting(%s): transcript not found, skipping", meeting_id)
            return None
        except RateLimitedError as e:
            with self._lock:
                self._backoff.record_rate_limit(e.retry_after)
            return None
        except FatalAPIError:
            with self._lock:
                self._backoff.record_fatal()
            return None
        except TransientAPIError as e:
            log.warning("watch_meeting(%s): transient error: %s", meeting_id, e)
            with self._lock:
                self._backoff.record_failure()
            return None
        except httpx.TimeoutException:
            log.warning("watch_meeting(%s): timeout", meeting_id)
            with self._lock:
                self._backoff.record_failure(is_timeout=True)
            return None
        except httpx.HTTPError as e:
            log.warning("watch_meeting(%s): HTTP error: %s", meeting_id, e)
            with self._lock:
                self._backoff.record_failure()
            return None

    def watch_meeting(self, meeting_id: str) -> bool:
        """Fetch a meeting by ID and add it to the entry map.

        Used by the Google Chat watcher to surface live meetings that the
        `transcripts` list query hides from non-admin users. If the entry is
        already known but not yet marked live, re-marks it live (the list
        cache typically loads with is_live=False). Returns True if the entry
        is present after the call, False on API error.
        """
        should_notify = False
        should_save_list = False
        with self._lock:
            existing = self._entries.get(meeting_id)
            if existing is not None:
                if not existing.meeting.is_live and not existing.meeting.is_completed:
                    meeting = existing.meeting.model_copy(update={"is_live": True})
                    self._entries[meeting_id] = MeetingEntry(
                        meeting=meeting, slug=existing.slug,
                    )
                    log.info(
                        "watch_meeting: re-marked %s %r as live",
                        meeting_id, meeting.title,
                    )
                    should_notify = True
                    should_save_list = True
                else:
                    return True
            elif self._backoff.is_backed_off:
                log.debug("watch_meeting(%s): backed off", meeting_id)
                return False
        if should_save_list:
            self._save_list_cache()
        if should_notify:
            self._notify_live_change(meeting_id)
            return True

        detail = self._fetch_detail_for_watch(meeting_id)
        if detail is None:
            return False

        meeting = _with_slug(detail.meeting)
        detail = detail.model_copy(update={"meeting": meeting})
        files = _render_files(meeting, detail)
        if _can_persist_detail(detail):
            self._status_cache.mark_completed(meeting_id)
            self._save_detail_to_disk(meeting_id, files)
        else:
            files[_IN_PROGRESS] = _render_in_progress_status(detail)

        with self._lock:
            self._backoff.record_success()
            detail, _live_state_changed = self._merge_live_stream_state_locked(meeting_id, detail)
            if meeting.id in self._entries:
                return True
            self._entries[meeting.id] = MeetingEntry(meeting=meeting, slug=meeting.slug)
            self._file_cache[meeting.id] = _CachedFiles(
                files=_render_files(detail.meeting, detail), fetched_at=time.monotonic(),
            )
            if not _can_persist_detail(detail):
                self._file_cache[meeting.id].files[_IN_PROGRESS] = _render_in_progress_status(detail)
        log.info(
            "watch_meeting: added %s %r (is_live=%s, date=%s)",
            meeting.id, meeting.title, meeting.is_live, meeting.date_str,
        )
        self._save_list_cache()
        self._notify_live_change(meeting_id)
        return True

    def sync_active_meeting_ids(self, active_ids: list[str]) -> None:
        """Mark meetings in the active_meetings query as live.

        Positive-only: we set is_live=True for IDs that appear in active_ids,
        but never flip is_live=True→False based on absence. The
        active_meetings query returns [] for non-admin users, so treating its
        absence as authoritative wipes out state set by watch_meeting /
        Google Chat discovery. Meetings transition out of live via terminal
        summary_status, handled in _fetch_meetings.
        """
        active = set(active_ids)
        notify_ids: list[str] = []
        with self._lock:
            for meeting_id in active:
                entry = self._entries.get(meeting_id)
                if entry is None or entry.meeting.is_live:
                    continue

                meeting = entry.meeting.model_copy(update={"is_live": True})
                self._entries[meeting_id] = MeetingEntry(meeting=meeting, slug=entry.slug)
                notify_ids.append(meeting_id)

                detail = self._live_details.get(meeting_id)
                if detail is None:
                    continue
                updated_detail = detail.model_copy(update={"meeting": meeting})
                self._live_details[meeting_id] = updated_detail
                self._cache_rendered_detail_locked(
                    meeting_id,
                    self._overlay_live_rows_locked(meeting_id, updated_detail),
                )
        for meeting_id in notify_ids:
            self._notify_live_change(meeting_id)

    def apply_live_transcript_update(
        self,
        meeting_id: str,
        transcript_id: str,
        sentence: Sentence,
    ) -> None:
        """Merge one realtime transcript row into the in-memory live render cache."""
        should_notify = False
        with self._lock:
            entry = self._entries.get(meeting_id)
            if entry is None:
                return

            detail = self._live_details.get(meeting_id)
            if detail is None:
                detail = TranscriptDetail(meeting=entry.meeting)

            rows = self._live_transcript_rows.setdefault(meeting_id, {})
            rows[transcript_id] = sentence

            baseline_detail = detail.model_copy(update={"meeting": entry.meeting})
            self._live_details[meeting_id] = baseline_detail
            self._cache_rendered_detail_locked(
                meeting_id,
                self._overlay_live_rows_locked(meeting_id, baseline_detail),
            )
            should_notify = True
        if should_notify:
            self._notify_live_change(meeting_id)

    def set_live_change_callback(self, callback: Callable[[str], None] | None) -> None:
        self._live_change_callback = callback

    def _notify_live_change(self, meeting_id: str) -> None:
        callback = self._live_change_callback
        if callback is not None:
            callback(meeting_id)

    def is_meeting_dynamic(self, meeting_id: str) -> bool:
        if self._status_cache.is_completed(meeting_id):
            return False
        with self._lock:
            entry = self._entries.get(meeting_id)
        return entry is not None and not entry.meeting.is_completed

    def get_meeting_paths(self, meeting_id: str) -> tuple[str, str | None, str | None] | None:
        with self._lock:
            entry = self._entries.get(meeting_id)
            if entry is None or not entry.meeting.date_str:
                return None

            date_str = entry.meeting.date_str
            dated_entries = [
                candidate
                for candidate in self._entries.values()
                if candidate.meeting.date_str == date_str
            ]
            datedir = next(
                (
                    dirname
                    for dirname, candidate in self._resolve_collisions(dated_entries).items()
                    if candidate.meeting.id == meeting_id
                ),
                None,
            )
            if datedir is None:
                return None

            year_month, day = date_str[:7], date_str[8:10]
            dated_path = f"/{year_month}/{day}/{datedir}"

            live_path: str | None = None
            if entry.meeting.is_live:
                live_entries = [candidate for candidate in self._entries.values() if candidate.meeting.is_live]
                live_dirname = next(
                    (
                        dirname
                        for dirname, candidate in self._resolve_collisions(live_entries).items()
                        if candidate.meeting.id == meeting_id
                    ),
                    None,
                )
                if live_dirname is not None:
                    live_path = f"/live/{live_dirname}"

            mine_path: str | None = None
            if self.user_email and entry.meeting.organizer_email == self.user_email:
                mine_entries = [
                    candidate
                    for candidate in self._entries.values()
                    if candidate.meeting.date_str == date_str
                    and candidate.meeting.organizer_email == self.user_email
                ]
                mine_dirname = next(
                    (
                        dirname
                        for dirname, candidate in self._resolve_collisions(mine_entries).items()
                        if candidate.meeting.id == meeting_id
                    ),
                    None,
                )
                if mine_dirname is not None:
                    mine_path = f"/mine/{year_month}/{day}/{mine_dirname}"

        return dated_path, live_path, mine_path

    def get_live_symlink_target(self, dirname: str) -> str | None:
        """Return the relative symlink target for a live meeting from /live/.

        Target is of the form ``../YYYY-MM/DD/<datedir>`` where ``datedir`` is
        the collision-resolved name in the full date tree. ``dirname`` is the
        slug-based name returned by ``list_live_dirnames``.
        """
        entry = self._live_entry_by_dirname(dirname)
        if entry is None:
            return None
        meeting_id = entry.meeting.id
        date_str = entry.meeting.date_str
        year_month, day = date_str[:7], date_str[8:10]
        for datedir, e in self.list_meetings(date_str).items():
            if e.meeting.id == meeting_id:
                return f"../{year_month}/{day}/{datedir}"
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

    def get_cached_file_content(self, meeting_id: str, filename: str) -> bytes | None:
        with self._lock:
            cached = self._file_cache.get(meeting_id)
            if cached is None:
                return None
            return cached.files.get(filename)

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

    @property
    def is_chat_auth_fatal(self) -> bool:
        """True if Google Chat credentials are missing or unrefreshable."""
        return self._chat_auth_fatal

    def mark_chat_auth_fatal(self) -> None:
        self._chat_auth_fatal = True

    def force_refresh(self) -> None:
        """Full cache invalidation: list, non-completed details, backoff."""
        with self._lock:
            self._list_cache_time = 0.0
            self._backoff = _BackoffState()
            self._chat_auth_fatal = False
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
