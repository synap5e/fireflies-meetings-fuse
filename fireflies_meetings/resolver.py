"""Pure reconciliation of every observation about one meeting.

The ordering in :data:`FIELD_PRECEDENCE` is intentionally data, rather than
being spread across projection, command, and store state machines.  Each
field selects the first candidate whose condition holds.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from .access_logs import AccessLogsOk, AccessLogsOutcome
from .models import AccessLogEntry, Meeting, MeetingInfo, Sentence, TranscriptDetail

log = logging.getLogger(__name__)

_TERMINAL_STATUSES = frozenset({"processed", "skipped", "missing_from_api"})
_ABANDON_AFTER_MS = 12 * 60 * 60 * 1000

type ResolvedState = Literal["live", "partial", "captured"]
type MeetingField = Literal[
    "id",
    "title",
    "date_epoch_ms",
    "duration_mins",
    "is_live",
    "organizer_email",
    "participants",
    "transcript_url",
    "slug",
    "meeting_info.summary_status",
    "meeting_info.fred_joined",
    "meeting_info.silent_meeting",
]


@dataclass(frozen=True)
class MeetingEvidence:
    """All current evidence used to resolve one canonical meeting."""

    list_meeting: Meeting | None
    detail: TranscriptDetail | None
    access_logs: AccessLogsOutcome
    live_captions: dict[str, Sentence]
    terminal_seen: bool
    now_ms: float


@dataclass(frozen=True)
class ResolvedMeeting:
    """Canonical meeting view plus its resolved transcript and state."""

    meeting: Meeting
    sentences: tuple[Sentence, ...]
    access_logs: tuple[AccessLogEntry, ...]
    state: ResolvedState
    provenance: MappingProxyType[str, str]


@dataclass(frozen=True)
class SourceCandidate:
    source: str
    condition: Callable[[MeetingEvidence], bool]
    extractor: Callable[[MeetingEvidence], object]


@dataclass(frozen=True)
class FieldRule:
    field: MeetingField
    candidates: tuple[SourceCandidate, ...]


def _list_present(evidence: MeetingEvidence) -> bool:
    return evidence.list_meeting is not None


def _detail_present(evidence: MeetingEvidence) -> bool:
    return evidence.detail is not None


def _list_title_set(evidence: MeetingEvidence) -> bool:
    return evidence.list_meeting is not None and bool(evidence.list_meeting.title)


def _list_date_set(evidence: MeetingEvidence) -> bool:
    return evidence.list_meeting is not None and evidence.list_meeting.date_epoch_ms > 0


def _detail_duration_set(evidence: MeetingEvidence) -> bool:
    return evidence.detail is not None and evidence.detail.meeting.duration_mins > 0


def _detail_status_set(evidence: MeetingEvidence) -> bool:
    return evidence.detail is not None and bool(evidence.detail.meeting.meeting_info.summary_status)


def _list_status_set(evidence: MeetingEvidence) -> bool:
    return evidence.list_meeting is not None and bool(evidence.list_meeting.meeting_info.summary_status)


def _resolved_status_before_age_out(evidence: MeetingEvidence) -> str:
    if _detail_status_set(evidence):
        assert evidence.detail is not None
        return evidence.detail.meeting.meeting_info.summary_status
    if _list_status_set(evidence):
        assert evidence.list_meeting is not None
        return evidence.list_meeting.meeting_info.summary_status
    return ""


def _looks_abandoned(evidence: MeetingEvidence) -> bool:
    """Whether an empty placeholder has exceeded the retry window."""
    if evidence.detail is None or _resolved_status_before_age_out(evidence):
        return False
    if (
        evidence.live_captions
        or (evidence.list_meeting is not None and evidence.list_meeting.is_live)
        or evidence.detail.meeting.is_live
    ):
        return False
    meeting = evidence.detail.meeting
    if meeting.duration_mins > 0 or evidence.detail.sentences:
        return False
    date_epoch_ms = (
        evidence.list_meeting.date_epoch_ms
        if evidence.list_meeting is not None and evidence.list_meeting.date_epoch_ms > 0
        else meeting.date_epoch_ms
    )
    return date_epoch_ms > 0 and evidence.now_ms - date_epoch_ms > _ABANDON_AFTER_MS


def _terminal_signal(evidence: MeetingEvidence) -> bool:
    return (
        evidence.terminal_seen
        or _resolved_status_before_age_out(evidence) in _TERMINAL_STATUSES
        or _looks_abandoned(evidence)
    )


def _always(_evidence: MeetingEvidence) -> bool:
    return True


def _never_live_if_terminal(evidence: MeetingEvidence) -> bool:
    return _terminal_signal(evidence)


def _list_id(evidence: MeetingEvidence) -> str:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.id


def _detail_id(evidence: MeetingEvidence) -> str:
    assert evidence.detail is not None
    return evidence.detail.meeting.id


def _list_title(evidence: MeetingEvidence) -> str:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.title


def _detail_title(evidence: MeetingEvidence) -> str:
    assert evidence.detail is not None
    return evidence.detail.meeting.title


def _list_date(evidence: MeetingEvidence) -> float:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.date_epoch_ms


def _detail_date(evidence: MeetingEvidence) -> float:
    assert evidence.detail is not None
    return evidence.detail.meeting.date_epoch_ms


def _detail_duration(evidence: MeetingEvidence) -> float:
    assert evidence.detail is not None
    return evidence.detail.meeting.duration_mins


def _list_duration(evidence: MeetingEvidence) -> float:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.duration_mins


def _list_live(evidence: MeetingEvidence) -> bool:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.is_live


def _detail_live(evidence: MeetingEvidence) -> bool:
    assert evidence.detail is not None
    return evidence.detail.meeting.is_live


def _list_organizer(evidence: MeetingEvidence) -> str:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.organizer_email


def _detail_organizer(evidence: MeetingEvidence) -> str:
    assert evidence.detail is not None
    return evidence.detail.meeting.organizer_email


def _list_participants(evidence: MeetingEvidence) -> list[str]:
    assert evidence.list_meeting is not None
    return list(evidence.list_meeting.participants)


def _detail_participants(evidence: MeetingEvidence) -> list[str]:
    assert evidence.detail is not None
    return list(evidence.detail.meeting.participants)


def _list_url(evidence: MeetingEvidence) -> str:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.transcript_url


def _detail_url(evidence: MeetingEvidence) -> str:
    assert evidence.detail is not None
    return evidence.detail.meeting.transcript_url


def _list_slug(evidence: MeetingEvidence) -> str:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.slug


def _detail_slug(evidence: MeetingEvidence) -> str:
    assert evidence.detail is not None
    return evidence.detail.meeting.slug


def _detail_status(evidence: MeetingEvidence) -> str:
    assert evidence.detail is not None
    return evidence.detail.meeting.meeting_info.summary_status


def _list_status(evidence: MeetingEvidence) -> str:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.meeting_info.summary_status


def _detail_fred_joined(evidence: MeetingEvidence) -> bool:
    assert evidence.detail is not None
    return evidence.detail.meeting.meeting_info.fred_joined


def _list_fred_joined(evidence: MeetingEvidence) -> bool:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.meeting_info.fred_joined


def _detail_silent(evidence: MeetingEvidence) -> bool:
    assert evidence.detail is not None
    return evidence.detail.meeting.meeting_info.silent_meeting


def _list_silent(evidence: MeetingEvidence) -> bool:
    assert evidence.list_meeting is not None
    return evidence.list_meeting.meeting_info.silent_meeting


# TODO(resolver-open-question-3): do not try to recognize placeholder detail
# titles; a nonempty list title wins unconditionally.
FIELD_PRECEDENCE: tuple[FieldRule, ...] = (
    FieldRule(
        "id",
        (
            SourceCandidate("list", _list_present, _list_id),
            SourceCandidate("detail", _detail_present, _detail_id),
        ),
    ),
    FieldRule(
        "title",
        (
            SourceCandidate("list", _list_title_set, _list_title),
            SourceCandidate("detail", _detail_present, _detail_title),
            SourceCandidate("list-empty", _list_present, _list_title),
        ),
    ),
    FieldRule(
        "date_epoch_ms",
        (
            SourceCandidate("list", _list_date_set, _list_date),
            SourceCandidate("detail", _detail_present, _detail_date),
            SourceCandidate("list-empty", _list_present, _list_date),
        ),
    ),
    FieldRule(
        "duration_mins",
        (
            SourceCandidate("detail", _detail_duration_set, _detail_duration),
            SourceCandidate("list", _list_present, _list_duration),
            SourceCandidate("detail-empty", _detail_present, _detail_duration),
        ),
    ),
    FieldRule(
        "is_live",
        (
            SourceCandidate("terminal", _never_live_if_terminal, lambda _evidence: False),
            SourceCandidate("list", _list_present, _list_live),
            SourceCandidate("detail", _detail_present, _detail_live),
        ),
    ),
    FieldRule(
        "organizer_email",
        (
            SourceCandidate("list", _list_present, _list_organizer),
            SourceCandidate("detail", _detail_present, _detail_organizer),
        ),
    ),
    FieldRule(
        "participants",
        (
            SourceCandidate("list", _list_present, _list_participants),
            SourceCandidate("detail", _detail_present, _detail_participants),
        ),
    ),
    FieldRule(
        "transcript_url",
        (
            SourceCandidate("list", _list_present, _list_url),
            SourceCandidate("detail", _detail_present, _detail_url),
        ),
    ),
    FieldRule(
        "slug",
        (
            SourceCandidate("list", _list_present, _list_slug),
            SourceCandidate("detail", _detail_present, _detail_slug),
        ),
    ),
    FieldRule(
        "meeting_info.summary_status",
        (
            SourceCandidate("detail", _detail_status_set, _detail_status),
            SourceCandidate("list", _list_status_set, _list_status),
            SourceCandidate("synthetic-age-out", _looks_abandoned, lambda _evidence: "missing_from_api"),
            SourceCandidate("empty", _always, lambda _evidence: ""),
        ),
    ),
    # TODO(resolver-open-question-4): detail always wins; no observation-order
    # rule is necessary for fred_joined.
    FieldRule(
        "meeting_info.fred_joined",
        (
            SourceCandidate("detail", _detail_present, _detail_fred_joined),
            SourceCandidate("list", _list_present, _list_fred_joined),
        ),
    ),
    FieldRule(
        "meeting_info.silent_meeting",
        (
            SourceCandidate("detail", _detail_present, _detail_silent),
            SourceCandidate("list", _list_present, _list_silent),
        ),
    ),
)


def _select(rule: FieldRule, evidence: MeetingEvidence) -> tuple[object, str]:
    for candidate in rule.candidates:
        if candidate.condition(evidence):
            return candidate.extractor(evidence), candidate.source
    raise ValueError(f"no resolver candidate matched {rule.field}")


# Track (meeting_id, list_status) pairs we've already warned about so a
# meeting that stays list-terminal/detail-empty across many rebuilds doesn't
# spam once per rebuild. Reset only on process restart — that's fine, the
# audit signal is "did we see this on this boot" which grep still surfaces.
_LIST_TERMINAL_WARNED: set[tuple[str, str]] = set()


def _warn_on_list_terminal_detail_empty(evidence: MeetingEvidence) -> None:
    if (
        evidence.list_meeting is None
        or evidence.detail is None
        or evidence.detail.meeting.meeting_info.summary_status
        or evidence.list_meeting.meeting_info.summary_status not in _TERMINAL_STATUSES
    ):
        return
    # TODO(resolver-open-question-1): list-terminal/detail-empty was not in the
    # audited corpus but shows up in production. Warn once per (id, status)
    # so the signal stays visible without storming the log on every rebuild.
    key = (evidence.list_meeting.id, evidence.list_meeting.meeting_info.summary_status)
    if key in _LIST_TERMINAL_WARNED:
        return
    _LIST_TERMINAL_WARNED.add(key)
    log.warning(
        "Resolver using terminal list status %s for %s because detail status is empty",
        evidence.list_meeting.meeting_info.summary_status,
        evidence.list_meeting.id,
    )


def _resolved_sentences(
    evidence: MeetingEvidence,
    *,
    terminal: bool,
) -> tuple[tuple[Sentence, ...], str]:
    detail_sentences = evidence.detail.sentences if evidence.detail is not None else []
    if terminal or not evidence.live_captions:
        return tuple(detail_sentences), "detail"
    merged = {str(sentence.index): sentence for sentence in detail_sentences}
    merged.update(evidence.live_captions)
    return (
        tuple(sorted(merged.values(), key=lambda sentence: (sentence.start_time, sentence.index))),
        "detail+live-captions" if detail_sentences else "live-captions",
    )


def resolve_meeting(evidence: MeetingEvidence) -> ResolvedMeeting:
    """Fold all evidence into a deterministic canonical meeting."""
    if evidence.list_meeting is None and evidence.detail is None:
        raise ValueError("meeting resolution requires list or detail evidence")

    _warn_on_list_terminal_detail_empty(evidence)
    values: dict[str, object] = {}
    provenance: dict[str, str] = {}
    for rule in FIELD_PRECEDENCE:
        value, source = _select(rule, evidence)
        values[rule.field] = value
        provenance[rule.field] = source

    meeting_info = MeetingInfo(
        summary_status=str(values["meeting_info.summary_status"]),
        fred_joined=bool(values["meeting_info.fred_joined"]),
        silent_meeting=bool(values["meeting_info.silent_meeting"]),
    )
    meeting = Meeting.model_validate({
        "id": values["id"],
        "title": values["title"],
        "date_epoch_ms": values["date_epoch_ms"],
        "duration_mins": values["duration_mins"],
        "is_live": values["is_live"],
        "organizer_email": values["organizer_email"],
        "participants": values["participants"],
        "transcript_url": values["transcript_url"],
        "slug": values["slug"],
        "meeting_info": meeting_info,
    })
    terminal = evidence.terminal_seen or meeting.summary_is_terminal
    sentences, sentence_source = _resolved_sentences(evidence, terminal=terminal)
    provenance["sentences"] = sentence_source

    if isinstance(evidence.access_logs, AccessLogsOk):
        access_logs = evidence.access_logs.logs
        provenance["access_logs"] = "access-logs-ok"
    else:
        access_logs = ()
        provenance["access_logs"] = f"access-logs-{evidence.access_logs.outcome}"

    if terminal:
        state: ResolvedState = "captured" if isinstance(evidence.access_logs, AccessLogsOk) else "partial"
    elif meeting.is_live or evidence.live_captions:
        state = "live"
    else:
        state = "partial"
    provenance["state"] = "resolver-state-decision"

    return ResolvedMeeting(
        meeting=meeting,
        sentences=sentences,
        access_logs=access_logs,
        state=state,
        provenance=MappingProxyType(provenance),
    )


def merge_list_observation(previous: Meeting | None, current: Meeting) -> Meeting:
    """Apply sticky list-observation rules before replacing ``list.json``."""
    if previous is None:
        return current
    updates: dict[str, object] = {}
    if current.date_epoch_ms <= 0 < previous.date_epoch_ms:
        updates["date_epoch_ms"] = previous.date_epoch_ms
        updates["date_str"] = previous.date_str
    if previous.is_live and not current.is_live and not current.summary_is_terminal:
        updates["is_live"] = True
    previous_status = previous.meeting_info.summary_status
    current_status = current.meeting_info.summary_status
    if previous_status in _TERMINAL_STATUSES and current_status not in _TERMINAL_STATUSES:
        updates["meeting_info"] = current.meeting_info.model_copy(
            update={"summary_status": previous_status},
        )
    return current if not updates else current.model_copy(update=updates)


def merge_detail_observation(
    previous: TranscriptDetail | None,
    current: TranscriptDetail,
) -> TranscriptDetail:
    """Keep a terminal detail status monotonic across latest-wins writes."""
    # TODO(resolver-open-question-2): ``missing_from_api`` is intentionally
    # reversible by a later real terminal status such as ``processed``.
    if previous is None:
        return current
    previous_status = previous.meeting.meeting_info.summary_status
    current_status = current.meeting.meeting_info.summary_status
    if previous_status not in _TERMINAL_STATUSES or current_status in _TERMINAL_STATUSES:
        return current
    meeting_info = current.meeting.meeting_info.model_copy(
        update={"summary_status": previous_status},
    )
    return current.model_copy(
        update={
            "meeting": current.meeting.model_copy(update={"meeting_info": meeting_info}),
        }
    )
