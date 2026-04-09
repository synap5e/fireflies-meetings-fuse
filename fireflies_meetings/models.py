"""Data models for Fireflies.ai meetings.

Pydantic BaseModels at the I/O boundary — they parse the Fireflies GraphQL
JSON responses directly (via field aliases) and also round-trip through the
disk cache via `model_dump_json` / `model_validate_json`. All models are
frozen so any "mutation" must go through `model_copy(update=...)`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

# Standard config for every model in this file: frozen, accept either the
# field name or its API alias on input, ignore unknown fields so we can pass
# the flat detail dict straight through to TranscriptDetail.
_MODEL_CONFIG = ConfigDict(
    frozen=True,
    populate_by_name=True,
    extra="ignore",
)

# Statuses Fireflies returns once a meeting has reached a terminal state.
# - "processed": summary generated successfully (the common case)
# - "skipped":   Fireflies decided not to summarize (usually short / single-attendee
#                meetings). Still terminal — no summary will ever appear.
# Anything else (e.g. "" for in-flight, "processing", "failed") is in-flight and
# the meeting should be re-fetched on subsequent reads.
_TERMINAL_STATUSES = frozenset({"processed", "skipped"})


class _FFBaseModel(BaseModel):
    """Base for every Fireflies data model.

    Drops `None` values from input dicts before validation, so explicit nulls
    in API responses fall back to the field default. The Fireflies API returns
    `null` for fields it considers "not applicable" — e.g. `sentences: null`
    and `meeting_info.fred_joined: null` on `summary_status: "skipped"`
    meetings — and Pydantic refuses to coerce `None` into `bool`/`list`/`str`.
    """

    model_config = _MODEL_CONFIG

    @model_validator(mode="before")
    @classmethod
    def _drop_nones(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        typed = cast("dict[str, object]", data)
        return {k: v for k, v in typed.items() if v is not None}


class Speaker(_FFBaseModel):
    """A meeting participant who spoke."""

    id: int = 0
    name: str = ""


class Sentence(_FFBaseModel):
    """A single sentence in the transcript."""

    index: int = 0
    text: str = ""
    start_time: float = 0.0  # seconds from meeting start
    end_time: float = 0.0  # seconds from meeting start
    speaker_name: str = ""


class Summary(_FFBaseModel):
    """AI-generated summary data for a meeting.

    Fireflies is inconsistent about the shape of these fields — `keywords`
    in particular comes back as a `list[str]` for some meetings and a `str`
    for others. We coerce lists to comma-separated strings so the renderer
    only ever sees text.
    """

    keywords: str = ""
    action_items: str = ""
    overview: str = ""
    gist: str = ""
    short_summary: str = ""

    @field_validator(
        "keywords", "action_items", "overview", "gist", "short_summary",
        mode="before",
    )
    @classmethod
    def _coerce_to_str(cls, v: object) -> object:
        if isinstance(v, list):
            items = cast("list[object]", v)
            return ", ".join(str(x) for x in items if x is not None)
        return v


class MeetingAttendee(_FFBaseModel):
    """A meeting attendee."""

    display_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("display_name", "displayName"),
    )
    email: str = ""


class MeetingInfo(_FFBaseModel):
    """Metadata about the Fireflies bot's participation."""

    fred_joined: bool = False
    silent_meeting: bool = False
    summary_status: str = ""


def _epoch_ms_to_date_str(epoch_ms: float) -> str:
    if not epoch_ms:
        return ""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC).strftime("%Y-%m-%d")


class Meeting(_FFBaseModel):
    """A Fireflies.ai meeting.

    Validates directly from the API JSON shape (`date`, `duration` are aliased
    to `date_epoch_ms` / `duration_mins`). `date_str` is derived from the epoch
    if not supplied. `slug` is computed by the store and stored via
    `model_copy(update={"slug": ...})`.
    """

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
        """True if the meeting has reached a terminal state.

        "processed" = summary generated. "skipped" = Fireflies declined to
        summarize but the meeting itself is done; treat as terminal so the
        backfill loop caches it instead of re-fetching forever.
        """
        return not self.is_live and self.meeting_info.summary_status in _TERMINAL_STATUSES


class TranscriptDetail(_FFBaseModel):
    """Full transcript data for a meeting.

    The Fireflies API returns meeting fields and transcript fields as siblings
    of one flat object; the API client (`api._nest_meeting_fields`) wraps that
    flat dict before passing it here, so this model only ever sees the nested
    shape.
    """

    meeting: Meeting
    sentences: list[Sentence] = Field(default_factory=list)
    speakers: list[Speaker] = Field(default_factory=list)
    summary: Summary | None = None
    attendees: list[MeetingAttendee] = Field(
        default_factory=list,
        validation_alias=AliasChoices("attendees", "meeting_attendees"),
    )
