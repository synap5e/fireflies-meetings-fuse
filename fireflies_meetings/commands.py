"""Single-writer commands for capture + projection updates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import trio

from .capture import CaptureStore
from .models import AccessLogEntry, Meeting, Sentence, TranscriptDetail
from .projection import (
    BackfillDiagnostic,
    Projection,
    ProjectionBuildOptions,
    build_projection_from_captures,
)


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
    logs: list[AccessLogEntry]


@dataclass(frozen=True)
class LiveCaptionArrived:
    name: Literal["live-caption-arrived"]
    meeting_id: str
    sentence: Sentence


@dataclass(frozen=True)
class StatusSupplemented:
    name: Literal["status-supplemented"]
    meetings: list[Meeting]


Command = ListRefreshed | DetailFetched | AccessLogsFetched | LiveCaptionArrived | StatusSupplemented


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
        self._diagnostics: dict[str, BackfillDiagnostic] = {}
        self._auth_fatal = False
        self._chat_auth_fatal = False
        self.projection = projection or build_projection_from_captures(
            capture.read_snapshot(),
            ProjectionBuildOptions(user_email=user_email),
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
            self._capture.write_list(command.meetings, fetched_at=fetched_at)
        elif isinstance(command, StatusSupplemented):
            existing = {meeting.id: meeting for meeting in self._capture.read_list()}
            for meeting in command.meetings:
                existing.setdefault(meeting.id, meeting)
            self._capture.write_list(list(existing.values()), fetched_at=fetched_at)
        elif isinstance(command, DetailFetched):
            self._capture.write_detail(command.meeting_id, command.detail)
            if command.detail.meeting.summary_is_terminal:
                self._live_captions.pop(command.meeting_id, None)
            invalidate_meeting_id = command.meeting_id
        elif isinstance(command, AccessLogsFetched):
            self._capture.write_access_logs(command.meeting_id, command.logs)
            invalidate_meeting_id = command.meeting_id
        else:
            projected = self.projection.meetings.get(command.meeting_id)
            if projected is not None and projected.capture_state == "captured":
                return self.projection, None
            rows = self._live_captions.setdefault(command.meeting_id, {})
            rows[str(command.sentence.index)] = command.sentence
            invalidate_meeting_id = command.meeting_id
        self._rebuild()
        return self.projection, invalidate_meeting_id

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
                diagnostics=self._diagnostics,
                auth_fatal=self._auth_fatal,
                chat_auth_fatal=self._chat_auth_fatal,
            ),
        )
