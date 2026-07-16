"""Compatibility facade over the CQRS capture/projection layers."""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx

from .api import (
    FatalAPIError,
    FirefliesClient,
    RateLimitedError,
    TranscriptNotFoundError,
    TransientAPIError,
)
from .capture import CaptureStore
from .commands import (
    AccessLogsFetched,
    ChannelsRefreshed,
    CommandProcessor,
    DetailFetched,
    ListRefreshed,
    LiveCaptionArrived,
    StatusSupplemented,
)
from .models import Meeting, Sentence, TranscriptDetail
from .projection import MEETING_FILES, Projection
from .slug import slugify
from .status_cache import StatusCache

log = logging.getLogger(__name__)

_BACKOFF_INITIAL = 30.0
_BACKOFF_MAX = 900.0
_BACKOFF_JITTER = 0.25


@dataclass(frozen=True)
class MeetingEntry:
    meeting: Meeting
    slug: str


@dataclass
class _BackoffState:
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
        self.delay = _BACKOFF_INITIAL if self.delay == 0.0 else min(self.delay * 2, _BACKOFF_MAX)
        jitter = self.delay * _BACKOFF_JITTER * (2 * random.random() - 1)
        self.until = time.monotonic() + self.delay + jitter

    def record_rate_limit(self, retry_after: float | None) -> None:
        self.delay = retry_after if retry_after and retry_after > 0 else min(
            max(self.delay * 2, _BACKOFF_INITIAL),
            _BACKOFF_MAX,
        )
        jitter = self.delay * _BACKOFF_JITTER * random.random()
        self.until = time.monotonic() + self.delay + jitter
        self.consecutive_timeouts = 0

    def record_fatal(self) -> None:
        self.fatal = True

    @property
    def is_backed_off(self) -> bool:
        return self.fatal or time.monotonic() < self.until


def _with_slug(meeting: Meeting) -> Meeting:
    if meeting.slug:
        return meeting
    return meeting.model_copy(update={"slug": slugify(meeting.title) if meeting.title else meeting.id[:12]})


def _merge_refresh_entry(existing: MeetingEntry | None, new_entry: MeetingEntry) -> MeetingEntry:
    if existing is None:
        return new_entry
    overrides: dict[str, object] = {}
    if (
        existing.meeting.is_live
        and not new_entry.meeting.is_live
        and not new_entry.meeting.summary_is_terminal
    ):
        overrides["is_live"] = True
    if new_entry.meeting.date_epoch_ms == 0 and existing.meeting.date_epoch_ms > 0:
        overrides["date_epoch_ms"] = existing.meeting.date_epoch_ms
        overrides["date_str"] = existing.meeting.date_str
    if not overrides:
        return new_entry
    meeting = new_entry.meeting.model_copy(update=overrides)
    return MeetingEntry(meeting=meeting, slug=meeting.slug)


class MeetingStore:
    """Stateful write facade; read methods serve the current projection only."""

    def __init__(
        self,
        client: FirefliesClient,
        *,
        list_ttl: float = 1800.0,
        status_cache: StatusCache | None = None,
        user_email: str | None = None,
    ) -> None:
        self._client = client
        self._status_cache = status_cache or StatusCache()
        self._capture = CaptureStore(self._status_cache.cache_dir)
        self.user_email = user_email
        self._processor = CommandProcessor(self._capture, user_email=user_email)
        self._projection: Projection = self._processor.projection
        self._backoff = _BackoffState()
        self._list_ttl = list_ttl
        self._list_cache_time = 0.0
        self._current_ttl = list_ttl
        self._channels_ttl = list_ttl * 6  # channels move slowly; refresh 6x rarer
        self._channels_cache_time = 0.0
        self._lock = threading.RLock()
        self._live_change_callback: Callable[[str], None] | None = None
        self._chat_auth_fatal = False

    @property
    def projection(self) -> Projection:
        return self._projection

    def _apply_projection(self) -> None:
        self._projection = self._processor.projection

    def _apply_command(self, command: object) -> str | None:
        with self._lock:
            if isinstance(
                command,
                (
                    ListRefreshed,
                    StatusSupplemented,
                    DetailFetched,
                    AccessLogsFetched,
                    LiveCaptionArrived,
                    ChannelsRefreshed,
                ),
            ):
                _projection, invalidated = self._processor.apply(command, fetched_at=time.time())
                self._apply_projection()
                return invalidated
        return None

    def _fetch_meetings(self, *, is_initial: bool) -> None:
        max_pages = None if is_initial else 1
        try:
            primary = self._client.list_transcripts(max_pages=max_pages)
            supplemental = self._client.list_recent_status_meetings()
        except RateLimitedError as e:
            self._backoff.record_rate_limit(e.retry_after)
            return
        except FatalAPIError:
            with self._lock:
                self._backoff.record_fatal()
                self._processor.set_auth_fatal(True)
                self._apply_projection()
            return
        except httpx.TimeoutException:
            self._backoff.record_failure(is_timeout=True)
            return
        except (TransientAPIError, httpx.HTTPError):
            self._backoff.record_failure()
            return

        existing = {
            meeting_id: MeetingEntry(meeting=item.meeting, slug=item.meeting.slug)
            for meeting_id, item in self._projection.meetings.items()
        }
        refreshed: list[Meeting] = []
        for meeting in primary:
            slugged = _with_slug(meeting)
            merged = _merge_refresh_entry(existing.get(slugged.id), MeetingEntry(slugged, slugged.slug))
            refreshed.append(merged.meeting)
        if is_initial:
            self._apply_command(ListRefreshed(name="list-refreshed", meetings=refreshed))
        else:
            merged_by_id = {item.meeting.id: item.meeting for item in existing.values()}
            merged_by_id.update({meeting.id: meeting for meeting in refreshed})
            self._apply_command(ListRefreshed(name="list-refreshed", meetings=list(merged_by_id.values())))
        if supplemental:
            self._apply_command(
                StatusSupplemented(
                    name="status-supplemented",
                    meetings=[_with_slug(m) for m in supplemental],
                ),
            )
        self._list_cache_time = time.time()
        self._current_ttl = self._list_ttl * (0.7 + random.random() * 0.6)
        self._backoff.record_success()

    def refresh_list_if_needed(self) -> None:
        if self._backoff.is_backed_off:
            return
        is_initial = not self._projection.meetings
        if not is_initial and time.time() - self._list_cache_time < self._current_ttl:
            return
        self._fetch_meetings(is_initial=is_initial)

    def refresh_channels_if_needed(self) -> None:
        """Refresh channels + memberships if the TTL has expired.

        Cheap in-memory guard first (TTL check), then two internal-API
        calls: getChannelsList + paginated fetchChannelMeetings("all").
        Silent on any failure — channels are decorative, not critical.
        """
        if self._backoff.is_backed_off:
            return
        if time.time() - self._channels_cache_time < self._channels_ttl:
            return
        try:
            channels = self._client.list_channels()
            memberships = self._client.list_channel_memberships([c.id for c in channels])
        except httpx.HTTPError as e:
            log.warning("Channels refresh failed: %s", e)
            return
        if memberships is None:
            return
        self._apply_command(
            ChannelsRefreshed(
                name="channels-refreshed",
                channels=channels,
                memberships=memberships,
            ),
        )
        self._channels_cache_time = time.time()
        log.info("Refreshed %d channels (%d with members)", len(channels), len(memberships))

    def mark_list_cache_fresh(self) -> None:
        self._list_cache_time = time.time()

    def list_year_months(self) -> list[str]:
        months = {
            item.meeting.date_str[:7]
            for item in self._projection.meetings.values()
            if item.primary_path is not None and item.meeting.date_str
        }
        return sorted(months, reverse=True)

    def list_days(self, year_month: str) -> list[str]:
        days = {
            item.meeting.date_str[8:10]
            for item in self._projection.meetings.values()
            if item.primary_path is not None
            and item.meeting.date_str
            and item.meeting.date_str[:7] == year_month
        }
        return sorted(days, reverse=True)

    def list_meetings(self, date_str: str) -> dict[str, MeetingEntry]:
        result: dict[str, MeetingEntry] = {}
        prefix = f"/{date_str[:7]}/{date_str[8:10]}/"
        for item in self._projection.meetings.values():
            if item.primary_path is None or not item.primary_path.startswith(prefix):
                continue
            dirname = item.primary_path.removeprefix(prefix)
            result[dirname] = MeetingEntry(meeting=item.meeting, slug=item.meeting.slug)
        return dict(sorted(result.items()))

    def list_year_months_mine(self) -> list[str]:
        months = {
            item.meeting.date_str[:7]
            for item in self._projection.meetings.values()
            if item.mine_path is not None and item.meeting.date_str
        }
        return sorted(months, reverse=True)

    def list_days_mine(self, year_month: str) -> list[str]:
        days = {
            item.meeting.date_str[8:10]
            for item in self._projection.meetings.values()
            if item.mine_path is not None
            and item.meeting.date_str
            and item.meeting.date_str[:7] == year_month
        }
        return sorted(days, reverse=True)

    def list_meetings_mine(self, date_str: str) -> dict[str, MeetingEntry]:
        result: dict[str, MeetingEntry] = {}
        prefix = f"/mine/{date_str[:7]}/{date_str[8:10]}/"
        for item in self._projection.meetings.values():
            if item.mine_path is None or not item.mine_path.startswith(prefix):
                continue
            dirname = item.mine_path.removeprefix(prefix)
            result[dirname] = MeetingEntry(meeting=item.meeting, slug=item.meeting.slug)
        return dict(sorted(result.items()))

    def list_live_dirnames(self) -> list[str]:
        return sorted(self._projection.live_dirnames)

    def get_live_symlink_target(self, dirname: str) -> str | None:
        node = self._projection.node(f"/live/{dirname}")
        return node.target.decode() if node is not None and node.kind == "symlink" else None

    def get_uncached_meeting_ids(self) -> list[str]:
        return [
            meeting_id for meeting_id, item in self._projection.meetings.items()
            if item.capture_state != "captured"
        ]

    def backfill_one(self, meeting_id: str) -> None:
        item = self._projection.meetings.get(meeting_id)
        if item is None or item.capture_state == "captured" or self._backoff.is_backed_off:
            return
        try:
            detail = self._client.get_transcript(meeting_id)
        except TranscriptNotFoundError:
            detail = TranscriptDetail(meeting=item.meeting.model_copy(update={
                "meeting_info": item.meeting.meeting_info.model_copy(update={"summary_status": "missing_from_api"}),
            }))
        except RateLimitedError as e:
            self._backoff.record_rate_limit(e.retry_after)
            raise
        except FatalAPIError:
            with self._lock:
                self._backoff.record_fatal()
                self._processor.set_auth_fatal(True)
                self._apply_projection()
            raise
        except TransientAPIError:
            self._backoff.record_failure()
            raise
        except httpx.TimeoutException:
            self._backoff.record_failure(is_timeout=True)
            raise
        except httpx.HTTPError:
            self._backoff.record_failure()
            raise
        detail = detail.model_copy(update={"meeting": detail.meeting.model_copy(update={
            "slug": item.meeting.slug,
            "date_str": item.meeting.date_str,
            "date_epoch_ms": item.meeting.date_epoch_ms,
        })})
        self._apply_command(DetailFetched(name="detail-fetched", meeting_id=meeting_id, detail=detail))
        if detail.meeting.is_completed:
            logs = detail.access_logs or self._client.get_access_logs(meeting_id)
            self._apply_command(AccessLogsFetched(name="access-logs-fetched", meeting_id=meeting_id, logs=list(logs)))
        self._backoff.record_success()
        self._notify_live_change(meeting_id)

    def watch_meeting(self, meeting_id: str) -> bool:
        item = self._projection.meetings.get(meeting_id)
        if item is not None:
            if not item.meeting.is_live and not item.meeting.is_completed:
                meetings = [
                    projected.meeting.model_copy(
                        update={"is_live": projected.meeting.id == meeting_id or projected.meeting.is_live},
                    )
                    for projected in self._projection.meetings.values()
                ]
                self._apply_command(ListRefreshed(name="list-refreshed", meetings=meetings))
                self._notify_live_change(meeting_id)
            return True
        if self._backoff.is_backed_off:
            return False
        try:
            detail = self._client.get_transcript(meeting_id)
        except (TranscriptNotFoundError, RateLimitedError, FatalAPIError, TransientAPIError, httpx.HTTPError):
            return False
        meeting = _with_slug(detail.meeting.model_copy(update={"is_live": True}))
        existing = [projected.meeting for projected in self._projection.meetings.values()]
        self._apply_command(ListRefreshed(name="list-refreshed", meetings=[*existing, meeting]))
        self._apply_command(
            DetailFetched(
                name="detail-fetched",
                meeting_id=meeting_id,
                detail=detail.model_copy(update={"meeting": meeting}),
            ),
        )
        self._notify_live_change(meeting_id)
        return True

    def sync_active_meeting_ids(self, active_ids: list[str]) -> None:
        active = set(active_ids)
        meetings = [
            item.meeting.model_copy(update={"is_live": True})
            if item.meeting.id in active and not item.meeting.is_completed
            else item.meeting
            for item in self._projection.meetings.values()
        ]
        if meetings:
            self._apply_command(ListRefreshed(name="list-refreshed", meetings=meetings))
        for meeting_id in active:
            self._notify_live_change(meeting_id)

    def apply_live_transcript_update(self, meeting_id: str, transcript_id: str, sentence: Sentence) -> None:
        _ = transcript_id
        invalidated = self._apply_command(
            LiveCaptionArrived(
                name="live-caption-arrived",
                meeting_id=meeting_id,
                sentence=sentence,
            ),
        )
        if invalidated is not None:
            self._notify_live_change(invalidated)

    def set_live_change_callback(self, callback: Callable[[str], None] | None) -> None:
        self._live_change_callback = callback

    def _notify_live_change(self, meeting_id: str) -> None:
        callback = self._live_change_callback
        if callback is not None:
            callback(meeting_id)

    def is_meeting_dynamic(self, meeting_id: str) -> bool:
        return self._projection.is_meeting_dynamic(meeting_id)

    def get_meeting_paths(self, meeting_id: str) -> tuple[str, str | None, str | None] | None:
        return self._projection.get_meeting_paths(meeting_id)

    def get_ghost_id(self, real_meeting_id: str) -> str | None:
        item = self._projection.meetings.get(real_meeting_id)
        return item.ghost_id if item is not None else None

    def get_ghost_file(self, real_meeting_id: str, filename: str) -> bytes | None:
        item = self._projection.meetings.get(real_meeting_id)
        if item is None or item.ghost_id is None:
            return None
        ghost = self._projection.meetings.get(item.ghost_id)
        return ghost.files.get(filename) if ghost is not None else None

    def get_ghost_file_size(self, real_meeting_id: str, filename: str) -> int:
        content = self.get_ghost_file(real_meeting_id, filename)
        return len(content) if content is not None else 0

    def get_overlap_ids(self, primary_id: str) -> list[str]:
        item = self._projection.meetings.get(primary_id)
        return list(item.overlap_ids) if item is not None else []

    def get_overlap_dirnames(self, primary_id: str) -> list[str]:
        item = self._projection.meetings.get(primary_id)
        return list(item.overlap_dirnames) if item is not None else []

    def get_overlap_id_for_dirname(self, primary_id: str, dirname: str) -> str | None:
        item = self._projection.meetings.get(primary_id)
        return item.overlap_dirnames.get(dirname) if item is not None else None

    def get_overlap_file(self, primary_id: str, overlap_dirname: str, filename: str) -> bytes | None:
        overlap_id = self.get_overlap_id_for_dirname(primary_id, overlap_dirname)
        if overlap_id is None:
            return None
        overlap = self._projection.meetings.get(overlap_id)
        return overlap.files.get(filename) if overlap is not None else None

    def get_overlap_file_size(self, primary_id: str, overlap_dirname: str, filename: str) -> int:
        content = self.get_overlap_file(primary_id, overlap_dirname, filename)
        return len(content) if content is not None else 0

    def get_overlap_warning(self, primary_id: str) -> bytes:
        overlap_ids = self.get_overlap_ids(primary_id)
        if not overlap_ids:
            return b"# Overlap Warning\n\nNo overlap recordings are currently folded under this recording.\n"
        return (
            b"# Overlap Warning\n\n"
            b"Overlap recordings are folded under this recording. "
            b"Open the overlap subdirectories to inspect their captured files.\n"
        )

    def get_file(self, meeting_id: str, filename: str) -> tuple[bytes | None, bool]:
        item = self._projection.meetings.get(meeting_id)
        if item is None:
            return None, False
        return item.files.get(filename), item.capture_state == "captured"

    def get_cached_file_content(self, meeting_id: str, filename: str) -> bytes | None:
        item = self._projection.meetings.get(meeting_id)
        return item.files.get(filename) if item is not None else None

    def get_file_size(self, meeting_id: str, filename: str) -> int:
        content = self.get_cached_file_content(meeting_id, filename)
        return len(content) if content is not None else 0

    def list_files(self, meeting_id: str) -> list[str]:
        return list(MEETING_FILES) if meeting_id in self._projection.meetings else []

    def invalidate(self) -> None:
        self._list_cache_time = 0.0

    @property
    def is_auth_fatal(self) -> bool:
        return self._backoff.fatal

    def backoff_remaining(self) -> float:
        if not self._backoff.is_backed_off or self._backoff.fatal:
            return 0.0
        return max(0.0, self._backoff.until - time.monotonic())

    @property
    def is_chat_auth_fatal(self) -> bool:
        return self._chat_auth_fatal

    def mark_chat_auth_fatal(self) -> None:
        with self._lock:
            self._chat_auth_fatal = True
            self._processor.set_chat_auth_fatal(True)
            self._apply_projection()

    def force_refresh(self) -> None:
        with self._lock:
            self._list_cache_time = 0.0
            self._backoff = _BackoffState()
            self._chat_auth_fatal = False
            self._processor.set_auth_fatal(False)
            self._processor.set_chat_auth_fatal(False)
            self._apply_projection()

    def capture_dir(self) -> Path:
        return self._capture.cache_dir
