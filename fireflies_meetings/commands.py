"""Single-writer commands for capture + projection updates."""

from __future__ import annotations

import ctypes
import ctypes.util
import gc
import time
from dataclasses import dataclass
from typing import Literal

import trio

from .access_logs import AccessLogsOutcome
from .capture import CaptureStore
from .models import Channel, Meeting, Sentence, TranscriptDetail
from .projection import (
    OneMeetingEvidence,
    Projection,
    ProjectionBuildOptions,
    build_projection_from_captures,
    rebuild_one_meeting,
)
from .resolver import merge_detail_observation, merge_list_observation

_libc_name = ctypes.util.find_library("c")
_libc = ctypes.CDLL(_libc_name) if _libc_name else None


def _return_freed_arenas_to_os() -> None:
    """Release glibc heap arenas back to the OS after a projection rebuild.

    Each rebuild transiently allocates ~800 MB of Pydantic models; glibc
    grows its per-thread arenas to fit the peak and does not shrink them
    unless asked. Without this, RSS creeps up on every command even though
    Python has already released the objects.
    """
    gc.collect()
    if _libc is not None:
        _libc.malloc_trim(0)


@dataclass(frozen=True)
class ListRefreshed:
    name: Literal["list-refreshed"]
    meetings: list[Meeting]


@dataclass(frozen=True)
class DetailFetched:
    name: Literal["detail-fetched"]
    meeting_id: str
    detail: TranscriptDetail


@dataclass(frozen=True)
class AccessLogsFetched:
    name: Literal["access-logs-fetched"]
    meeting_id: str
    outcome: AccessLogsOutcome


@dataclass(frozen=True)
class LiveCaptionArrived:
    name: Literal["live-caption-arrived"]
    meeting_id: str
    sentence: Sentence


@dataclass(frozen=True)
class StatusSupplemented:
    name: Literal["status-supplemented"]
    meetings: list[Meeting]


@dataclass(frozen=True)
class ChannelsRefreshed:
    name: Literal["channels-refreshed"]
    channels: list[Channel]
    memberships: dict[str, list[str]]


Command = (
    ListRefreshed
    | DetailFetched
    | AccessLogsFetched
    | LiveCaptionArrived
    | StatusSupplemented
    | ChannelsRefreshed
)


class CommandProcessor:
    """Serial command applier.

    The async ``run`` method is the production single-writer loop. Tests and
    synchronous code can call ``apply`` directly; it uses the same transition
    logic.
    """

    def __init__(
        self,
        capture: CaptureStore,
        *,
        user_email: str | None = None,
        projection: Projection | None = None,
    ) -> None:
        self._capture = capture
        self._user_email = user_email
        self._live_captions: dict[str, dict[str, Sentence]] = {}
        self._auth_fatal = False
        self._chat_auth_fatal = False
        snapshot = capture.read_snapshot()
        self.projection = projection or build_projection_from_captures(
            snapshot,
            ProjectionBuildOptions(user_email=user_email, now_ms=time.time() * 1000),
        )
        self._send, self._receive = trio.open_memory_channel[Command](100)

    @property
    def sender(self) -> trio.MemorySendChannel[Command]:
        return self._send

    def set_auth_fatal(self, value: bool) -> None:
        self._auth_fatal = value
        self._rebuild()

    def set_chat_auth_fatal(self, value: bool) -> None:
        self._chat_auth_fatal = value
        self._rebuild()

    def apply(self, command: Command, *, fetched_at: float) -> tuple[Projection, str | None]:
        invalidate_meeting_id: str | None = None
        if isinstance(command, ListRefreshed):
            existing = {meeting.id: meeting for meeting in self._capture.read_list()}
            meetings = [
                merge_list_observation(existing.get(meeting.id), meeting)
                for meeting in command.meetings
            ]
            self._capture.write_list(meetings, fetched_at=fetched_at)
        elif isinstance(command, StatusSupplemented):
            existing = {meeting.id: meeting for meeting in self._capture.read_list()}
            for meeting in command.meetings:
                existing.setdefault(meeting.id, meeting)
            self._capture.write_list(list(existing.values()), fetched_at=fetched_at)
        elif isinstance(command, DetailFetched):
            previous = self._capture.read_detail(command.meeting_id)
            detail = merge_detail_observation(previous, command.detail)
            self._capture.write_detail(command.meeting_id, detail)
            invalidate_meeting_id = command.meeting_id
            if self._apply_one_meeting_fast_path(command.meeting_id):
                return self.projection, invalidate_meeting_id
        elif isinstance(command, AccessLogsFetched):
            self._capture.write_access_logs(command.meeting_id, command.outcome)
            invalidate_meeting_id = command.meeting_id
            if self._apply_one_meeting_fast_path(command.meeting_id):
                return self.projection, invalidate_meeting_id
        elif isinstance(command, ChannelsRefreshed):
            self._capture.write_channels(command.channels, command.memberships, fetched_at=fetched_at)
        else:
            rows = self._live_captions.setdefault(command.meeting_id, {})
            rows[str(command.sentence.index)] = command.sentence
            invalidate_meeting_id = command.meeting_id
            if self._apply_one_meeting_fast_path(command.meeting_id):
                return self.projection, invalidate_meeting_id
        self._rebuild()
        return self.projection, invalidate_meeting_id

    def _apply_one_meeting_fast_path(self, meeting_id: str) -> bool:
        """Incremental swap for one meeting's file bytes, skipping the O(N)
        full rebuild.

        Safe for any command that only changes one meeting's content —
        captions, detail fetches, access-log fetches — because none of
        them move meetings between slug groups or change primary paths,
        so fold groups and tree structure stay put.

        NOT safe for list refreshes or channels: those can add/drop
        meetings and reshape fold groups.

        Reads only THIS meeting's cached detail + access logs (a few KB)
        instead of the full snapshot (~160MB of JSON parse across 1780
        meeting dirs). The full-snapshot read was pinning CPU near 90%
        under normal backfill load.

        Returns True on success. Falls back to False (caller does full
        rebuild) when the meeting isn't in the current projection yet —
        e.g. a detail fetch arrived before its list refresh landed it."""
        existing = self.projection.meetings.get(meeting_id)
        if existing is None:
            return False
        evidence = OneMeetingEvidence(
            list_meeting=existing.meeting,
            detail_capture=self._capture.read_detail(meeting_id),
            access_logs=self._capture.read_access_log(meeting_id),
            live_captions=self._live_captions,
            now_ms=time.time() * 1000,
        )
        updated = rebuild_one_meeting(meeting_id, existing, evidence)
        self.projection = self.projection.replace_meeting(meeting_id, updated)
        return True

    async def send(self, command: Command) -> None:
        await self._send.send(command)

    async def run(self) -> None:
        async with self._receive:
            async for command in self._receive:
                self.apply(command, fetched_at=trio.current_time())

    def _rebuild(self) -> None:
        self.projection = build_projection_from_captures(
            self._capture.read_snapshot(),
            ProjectionBuildOptions(
                user_email=self._user_email,
                live_captions=self._live_captions,
                auth_fatal=self._auth_fatal,
                chat_auth_fatal=self._chat_auth_fatal,
                now_ms=time.time() * 1000,
            ),
        )
        _return_freed_arenas_to_os()
