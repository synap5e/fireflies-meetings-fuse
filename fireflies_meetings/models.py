"""Data models for Fireflies.ai meetings.

Pydantic BaseModels at the I/O boundary — they parse the Fireflies GraphQL
JSON responses directly (via field aliases) and also round-trip through the
disk cache via `model_dump_json` / `model_validate_json`. All models are
frozen so any "mutation" must go through `model_copy(update=...)`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator

# Standard config for every model in this file: frozen, accept either the
# field name or its API alias on input, ignore unknown fields so we can pass
# the flat detail dict straight through to TranscriptDetail.
_MODEL_CONFIG = ConfigDict(
    frozen=True,
    populate_by_name=True,
    extra="ignore",
)


class Speaker(BaseModel):
    """A meeting participant who spoke."""

    model_config = _MODEL_CONFIG

    id: int = 0
    name: str = ""


class Sentence(BaseModel):
    """A single sentence in the transcript."""

    model_config = _MODEL_CONFIG

    index: int = 0
    text: str = ""
    start_time: float = 0.0  # seconds from meeting start
    end_time: float = 0.0  # seconds from meeting start
    speaker_name: str = ""


class Summary(BaseModel):
    """AI-generated summary data for a meeting."""

    model_config = _MODEL_CONFIG

    keywords: str = ""
    action_items: str = ""
    overview: str = ""
    gist: str = ""
    short_summary: str = ""


class MeetingAttendee(BaseModel):
    """A meeting attendee."""

    model_config = _MODEL_CONFIG

    display_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("display_name", "displayName"),
    )
    email: str = ""


class MeetingInfo(BaseModel):
    """Metadata about the Fireflies bot's participation."""

    model_config = _MODEL_CONFIG

    fred_joined: bool = False
    silent_meeting: bool = False
    summary_status: str = ""


def _epoch_ms_to_date_str(epoch_ms: float) -> str:
    if not epoch_ms:
        return ""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC).strftime("%Y-%m-%d")


class Meeting(BaseModel):
    """A Fireflies.ai meeting.

    Validates directly from the API JSON shape (`date`, `duration` are aliased
    to `date_epoch_ms` / `duration_mins`). `date_str` is derived from the epoch
    if not supplied. `slug` is computed by the store and stored via
    `model_copy(update={"slug": ...})`.
    """

    model_config = _MODEL_CONFIG

    id: str = ""
    title: str = ""
    # API uses "date" (epoch ms); disk cache uses "date_epoch_ms"
    date_epoch_ms: float = Field(
        default=0.0,
        validation_alias=AliasChoices("date_epoch_ms", "date"),
    )
    date_str: str = ""  # YYYY-MM-DD, derived from date_epoch_ms when missing
    duration_mins: float = Field(
        default=0.0,
        validation_alias=AliasChoices("duration_mins", "duration"),
    )
    is_live: bool = False
    organizer_email: str = ""
    participants: list[str] = Field(default_factory=list)
    transcript_url: str = ""
    meeting_info: MeetingInfo = Field(default_factory=MeetingInfo)
    slug: str = ""  # computed by store, set via model_copy

    @model_validator(mode="before")
    @classmethod
    def _derive_date_str(cls, data: object) -> object:
        """Fill in `date_str` from `date_epoch_ms` / `date` if not provided."""
        if not isinstance(data, dict):
            return data
        typed = cast("dict[str, object]", data)
        if typed.get("date_str"):
            return typed
        epoch_raw = typed.get("date_epoch_ms") or typed.get("date")
        if isinstance(epoch_raw, (int, float)) and epoch_raw:
            new_data: dict[str, object] = dict(typed)
            new_data["date_str"] = _epoch_ms_to_date_str(float(epoch_raw))
            return new_data
        return typed

    @property
    def is_completed(self) -> bool:
        """True if the meeting is over and summary has been processed."""
        return not self.is_live and self.meeting_info.summary_status == "processed"


class TranscriptDetail(BaseModel):
    """Full transcript data for a meeting.

    The Fireflies API returns the meeting fields and the transcript fields
    as siblings of one flat object. We promote the meeting fields into a
    nested `Meeting` so the rest of the codebase has a clean separation.
    """

    model_config = _MODEL_CONFIG

    meeting: Meeting
    sentences: list[Sentence] = Field(default_factory=list)
    speakers: list[Speaker] = Field(default_factory=list)
    summary: Summary | None = None
    attendees: list[MeetingAttendee] = Field(
        default_factory=list,
        validation_alias=AliasChoices("attendees", "meeting_attendees"),
    )

    @model_validator(mode="before")
    @classmethod
    def _promote_flat_meeting(cls, data: object) -> object:
        """If the input is a flat API dict (no nested 'meeting' key), wrap it."""
        if not isinstance(data, dict):
            return data
        typed = cast("dict[str, object]", data)
        if "meeting" in typed:
            return typed
        new_data: dict[str, object] = dict(typed)
        new_data["meeting"] = typed
        return new_data
