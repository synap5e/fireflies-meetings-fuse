"""Data models for Fireflies.ai meetings.

Pydantic BaseModels at the I/O boundary — they parse the Fireflies GraphQL
JSON responses directly (via field aliases) and also round-trip through the
disk cache via `model_dump_json` / `model_validate_json`. All models are
frozen so any "mutation" must go through `model_copy(update=...)`.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import cast

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

# Allowlist for meeting IDs. Fireflies controls these but the rest of
# the codebase uses them as filesystem path components, so we validate
# at the boundary: alphanumerics, underscore, and dash, 1-64 chars.
_MEETING_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

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
# - "missing_from_api": Fireflies returned 404 / object_not_found for a previously
#                known transcript. Terminal, but the meeting should keep
#                showing as cached with a "gone from API" message.
# Anything else (e.g. "" for in-flight, "processing", "failed") is in-flight and
# the meeting should be re-fetched on subsequent reads.
_TERMINAL_STATUSES = frozenset({"processed", "skipped", "missing_from_api"})

type RawObjectDict = dict[str, object]


def _normalize_summary_status(value: object) -> object:
    if value == "completed":
        return "processed"
    if value == "not_found":
        return "missing_from_api"
    return value


def _speaker_name_from_candidate(candidate: object) -> str:
    if isinstance(candidate, str):
        return candidate
    if not isinstance(candidate, dict):
        return ""
    typed = cast("RawObjectDict", candidate)
    for key in ("speaker_name", "speakerName", "displayName", "name"):
        value = typed.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _speaker_name_from_speakers(speakers: object, speaker_key: str) -> str:
    if not isinstance(speakers, list):
        return ""
    for item in cast("list[object]", speakers):
        if not isinstance(item, dict):
            continue
        typed = cast("RawObjectDict", item)
        if str(typed.get("id")) != speaker_key:
            continue
        return _speaker_name_from_candidate(typed)
    return ""


def _speaker_name_from_meta(speaker_id: object, speaker_meta: object) -> str:
    if speaker_id is None:
        return ""
    speaker_key = str(speaker_id)
    if not isinstance(speaker_meta, dict):
        return speaker_key
    typed = cast("RawObjectDict", speaker_meta)
    candidate = typed.get(speaker_key)
    if candidate is None:
        candidate = typed.get(str(speaker_id))
    name = _speaker_name_from_candidate(candidate)
    if name:
        return name
    name = _speaker_name_from_speakers(typed.get("speakers"), speaker_key)
    if name:
        return name
    return speaker_key


def _internal_meeting_info(raw: RawObjectDict) -> RawObjectDict:
    audio_meta = raw.get("audioServiceMetadata")
    silent_meeting = False
    if isinstance(audio_meta, dict):
        typed_audio_meta = cast("RawObjectDict", audio_meta)
        silent_raw = typed_audio_meta.get("silentMeeting", False)
        if isinstance(silent_raw, bool):
            silent_meeting = silent_raw
    return {
        "silent_meeting": silent_meeting,
        "summary_status": _normalize_summary_status(
            raw.get("summaryStatus") or raw.get("processMeetingStatus") or "",
        ),
    }


def _internal_transcript_url(raw: RawObjectDict) -> str:
    parse_id = raw.get("parseId") or raw.get("_id") or raw.get("id")
    if isinstance(parse_id, str) and parse_id:
        return f"https://app.fireflies.ai/view/{parse_id}"
    return ""


def _internal_participants(raw: RawObjectDict) -> list[str] | None:
    all_emails = raw.get("allEmails")
    if isinstance(all_emails, str) and all_emails:
        return all_emails.split()
    return None


def _coerce_epoch_ms(raw: object) -> float | None:
    if isinstance(raw, (int, float)) and raw:
        return float(raw)
    if isinstance(raw, str) and raw:
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt.timestamp() * 1000
    return None


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
        typed = cast("RawObjectDict", data)
        return {k: v for k, v in typed.items() if v is not None}


class Speaker(_FFBaseModel):
    """A meeting participant who spoke."""

    id: int = 0
    name: str = ""


class Sentence(_FFBaseModel):
    """A single sentence in the transcript."""

    index: int = 0
    text: str = Field(default="", validation_alias=AliasChoices("text", "sentence"))
    start_time: float = Field(default=0.0, validation_alias=AliasChoices("start_time", "time"))
    end_time: float = Field(default=0.0, validation_alias=AliasChoices("end_time", "endTime"))
    speaker_name: str = Field(default="", validation_alias=AliasChoices("speaker_name", "speakerName"))


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
    silent_meeting: bool = Field(
        default=False,
        validation_alias=AliasChoices("silent_meeting", "silentMeeting"),
    )
    summary_status: str = Field(
        default="",
        validation_alias=AliasChoices("summary_status", "summaryStatus", "processMeetingStatus"),
    )

    @field_validator("summary_status", mode="before")
    @classmethod
    def _normalize_summary_status(cls, value: object) -> object:
        return _normalize_summary_status(value)


def _epoch_ms_to_date_str(epoch_ms: float) -> str:
    if not epoch_ms:
        return ""
    return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d")


class Meeting(_FFBaseModel):
    """A Fireflies.ai meeting.

    Validates directly from the API JSON shape (`date`, `duration` are aliased
    to `date_epoch_ms` / `duration_mins`). `date_str` is derived from the epoch
    if not supplied. `slug` is computed by the store and stored via
    `model_copy(update={"slug": ...})`.
    """

    id: str = Field(validation_alias=AliasChoices("id", "parseId", "_id"))
    title: str = ""
    # API uses "date" (epoch ms); disk cache uses "date_epoch_ms"
    date_epoch_ms: float = Field(
        default=0.0,
        validation_alias=AliasChoices("date_epoch_ms", "date"),
    )
    date_str: str = ""  # YYYY-MM-DD, derived from date_epoch_ms when missing
    duration_mins: float = Field(
        default=0.0,
        validation_alias=AliasChoices("duration_mins", "duration", "durationMins"),
    )
    is_live: bool = False
    organizer_email: str = Field(
        default="",
        validation_alias=AliasChoices("organizer_email", "creator_email"),
    )
    participants: list[str] = Field(default_factory=list)
    transcript_url: str = ""
    meeting_info: MeetingInfo = Field(default_factory=MeetingInfo)
    slug: str = ""  # computed by store, set via model_copy

    @field_validator("id")
    @classmethod
    def _validate_id(cls, v: str) -> str:
        """Reject IDs that aren't safe to use as a filesystem path component."""
        if not _MEETING_ID_PATTERN.match(v):
            raise ValueError(
                f"meeting id must match {_MEETING_ID_PATTERN.pattern}, got {v!r}"
            )
        return v

    @model_validator(mode="before")
    @classmethod
    def _derive_date_str(cls, data: object) -> object:
        """Always derive `date_str` from the epoch timestamp.

        Re-derives even when `date_str` is already present (e.g. from the
        disk cache) so that a timezone change takes effect without nuking
        the cache.
        """
        if not isinstance(data, dict):
            return data
        typed = cast("RawObjectDict", data)
        new_data: RawObjectDict = dict(typed)

        if "meeting_info" not in new_data:
            new_data["meeting_info"] = _internal_meeting_info(new_data)

        if not new_data.get("participants"):
            participants = _internal_participants(new_data)
            if participants is not None:
                new_data["participants"] = participants

        if not new_data.get("transcript_url"):
            transcript_url = _internal_transcript_url(new_data)
            if transcript_url:
                new_data["transcript_url"] = transcript_url

        epoch_ms = _coerce_epoch_ms(new_data.get("date_epoch_ms") or new_data.get("date"))
        if epoch_ms is not None:
            new_data["date_epoch_ms"] = epoch_ms
            new_data["date_str"] = _epoch_ms_to_date_str(epoch_ms)
            return new_data
        return new_data

    @property
    def is_completed(self) -> bool:
        """True if the meeting has reached a terminal state.

        "processed" = summary generated. "skipped" = Fireflies declined to
        summarize but the meeting itself is done; treat as terminal so the
        backfill loop caches it instead of re-fetching forever.
        """
        return not self.is_live and self.summary_is_terminal

    @property
    def summary_is_terminal(self) -> bool:
        """True if the API reports a terminal summary_status regardless of is_live.

        Used to decide whether a list refresh is allowed to flip a locally-held
        is_live=True flag back to False. Checking only summary_status avoids the
        circular dependency is_completed has with is_live.
        """
        return self.meeting_info.summary_status in _TERMINAL_STATUSES


class TranscriptDetail(_FFBaseModel):
    """Full transcript data for a meeting.

    The Fireflies API returns meeting fields and transcript fields as siblings
    of one flat object; the API client (`api._nest_meeting_fields`) wraps that
    flat dict before passing it here, so this model only ever sees the nested
    shape.
    """

    meeting: Meeting
    sentences: list[Sentence] = Field(
        default_factory=list,
        validation_alias=AliasChoices("sentences", "captions"),
    )
    speakers: list[Speaker] = Field(default_factory=list)
    summary: Summary | None = None
    attendees: list[MeetingAttendee] = Field(
        default_factory=list,
        validation_alias=AliasChoices("attendees", "meeting_attendees"),
    )
    # Internal, not from Fireflies: set when the API returns partial data but
    # fails a critical transcript field such as transcript.sentences.
    transcript_error: str = ""

    @model_validator(mode="before")
    @classmethod
    def _normalize_internal_captions(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        typed = cast("RawObjectDict", data)
        raw_captions = typed.get("captions")
        if not isinstance(raw_captions, list) or not raw_captions:
            return typed

        speaker_meta = typed.get("speakerMeta")
        normalized: list[object] = []
        changed = False
        for raw_caption in cast("list[object]", raw_captions):
            if not isinstance(raw_caption, dict):
                normalized.append(raw_caption)
                continue
            typed_caption = cast("RawObjectDict", raw_caption)
            caption: RawObjectDict = dict(typed_caption)
            if not caption.get("speaker_name"):
                caption["speaker_name"] = _speaker_name_from_meta(
                    typed_caption.get("speaker_id"), speaker_meta,
                )
                changed = True
            normalized.append(caption)

        if not changed:
            return typed
        new_data = dict(typed)
        new_data["captions"] = normalized
        return new_data
