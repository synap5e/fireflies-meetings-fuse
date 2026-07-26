from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from fireflies_meetings.access_logs import (
    ACCESS_LOGS_FAILED,
    ACCESS_LOGS_PENDING,
    AccessLogsOutcome,
    access_logs_ok,
)
from fireflies_meetings.models import AccessLogEntry, Meeting, MeetingInfo, Sentence, TranscriptDetail
from fireflies_meetings.resolver import (
    MeetingEvidence,
    ResolvedMeeting,
    merge_detail_observation,
    merge_list_observation,
    resolve_meeting,
)

_NOW_MS = 1_800_000_000_000.0
_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "resolver"


def _meeting(**updates: object) -> Meeting:
    meeting_id = cast("str", updates.pop("meeting_id", "MEET01"))
    summary_status = cast("str", updates.pop("summary_status", ""))
    fred_joined = cast("bool", updates.pop("fred_joined", False))
    silent_meeting = cast("bool", updates.pop("silent_meeting", False))
    values: dict[str, object] = {
        "id": meeting_id,
        "title": "List title",
        "date_epoch_ms": _NOW_MS - 60_000,
        "duration_mins": 30,
        "is_live": False,
        "organizer_email": "list@example.com",
        "participants": ["list@example.com"],
        "transcript_url": "https://example.test/list",
        "slug": "list-title",
        "meeting_info": MeetingInfo(
            summary_status=summary_status,
            fred_joined=fred_joined,
            silent_meeting=silent_meeting,
        ),
    }
    values.update(updates)
    return Meeting.model_validate(values)


def _detail(
    meeting: Meeting | None = None,
    *,
    sentences: list[Sentence] | None = None,
) -> TranscriptDetail:
    target = meeting or _meeting(
        title="Detail title",
        date_epoch_ms=_NOW_MS - 120_000,
        duration_mins=25,
        organizer_email="detail@example.com",
        participants=["detail@example.com"],
        transcript_url="https://example.test/detail",
        slug="detail-title",
        fred_joined=True,
        silent_meeting=True,
    )
    return TranscriptDetail(meeting=target, sentences=[] if sentences is None else sentences)


def _evidence(
    list_meeting: Meeting | None,
    detail: TranscriptDetail | None,
    **updates: object,
) -> MeetingEvidence:
    live_captions = cast("dict[str, Sentence] | None", updates.get("live_captions"))
    return MeetingEvidence(
        list_meeting=list_meeting,
        detail=detail,
        access_logs=cast("AccessLogsOutcome", updates.get("access_logs", ACCESS_LOGS_PENDING)),
        live_captions={} if live_captions is None else live_captions,
        terminal_seen=cast("bool", updates.get("terminal_seen", False)),
        now_ms=cast("float", updates.get("now_ms", _NOW_MS)),
    )


def _sentence(index: int, text: str) -> Sentence:
    return Sentence(index=index, text=text, start_time=float(index), end_time=float(index + 1))


def test_id_list_is_immutable_key_and_detail_is_fallback() -> None:
    listed = _meeting(meeting_id="LIST01")
    mismatched = _detail(_meeting(meeting_id="DETAIL01"))
    assert resolve_meeting(_evidence(listed, mismatched)).meeting.id == "LIST01"
    assert resolve_meeting(_evidence(None, mismatched)).meeting.id == "DETAIL01"


def test_title_list_wins_and_empty_list_falls_back_to_detail() -> None:
    detail = _detail()
    assert resolve_meeting(_evidence(_meeting(title="Real title"), detail)).meeting.title == "Real title"
    assert resolve_meeting(_evidence(_meeting(title=""), detail)).meeting.title == "Detail title"


def test_date_list_precision_wins_and_nonpositive_list_falls_back_to_detail() -> None:
    detail = _detail()
    assert resolve_meeting(_evidence(_meeting(date_epoch_ms=_NOW_MS - 1), detail)).meeting.date_epoch_ms == _NOW_MS - 1
    resolved = resolve_meeting(_evidence(_meeting(date_epoch_ms=0), detail)).meeting
    assert resolved.date_epoch_ms == detail.meeting.date_epoch_ms
    assert resolved.date_str == detail.meeting.date_str


def test_date_and_live_list_observations_are_sticky_only_when_allowed() -> None:
    previous = _meeting(date_epoch_ms=_NOW_MS - 50, is_live=True)
    regressed = _meeting(date_epoch_ms=0, is_live=False)
    merged = merge_list_observation(previous, regressed)
    assert merged.date_epoch_ms == previous.date_epoch_ms
    assert merged.is_live

    fresh = _meeting(date_epoch_ms=_NOW_MS, is_live=False, summary_status="processed")
    merged_fresh = merge_list_observation(previous, fresh)
    assert merged_fresh.date_epoch_ms == _NOW_MS
    assert not merged_fresh.is_live


def test_duration_positive_detail_wins_and_zero_detail_falls_back_to_list() -> None:
    listed = _meeting(duration_mins=60)
    assert resolve_meeting(_evidence(listed, _detail(_meeting(duration_mins=12)))).meeting.duration_mins == 12
    assert resolve_meeting(_evidence(listed, _detail(_meeting(duration_mins=0)))).meeting.duration_mins == 60


def test_live_uses_list_until_terminal_evidence_forces_false() -> None:
    listed = _meeting(is_live=True)
    nonterminal = _detail(_meeting(is_live=False))
    assert resolve_meeting(_evidence(listed, nonterminal)).meeting.is_live

    terminal = _detail(_meeting(is_live=True, summary_status="processed"))
    resolved_terminal = resolve_meeting(_evidence(listed, terminal)).meeting
    assert not resolved_terminal.is_live
    assert resolved_terminal.is_completed
    assert not resolve_meeting(_evidence(listed, nonterminal, terminal_seen=True)).meeting.is_live


@pytest.mark.parametrize(
    ("attribute", "list_value", "detail_value"),
    [
        ("organizer_email", "list@example.com", "detail@example.com"),
        ("transcript_url", "https://example.test/list", "https://example.test/detail"),
        ("slug", "list-slug", "detail-slug"),
    ],
)
def test_list_metadata_wins_with_detail_only_fallback(
    attribute: str,
    list_value: str,
    detail_value: str,
) -> None:
    listed = _meeting(
        organizer_email=list_value if attribute == "organizer_email" else "same@example.com",
        transcript_url=list_value if attribute == "transcript_url" else "https://example.test/same",
        slug=list_value if attribute == "slug" else "same",
    )
    detailed_meeting = _meeting(
        organizer_email=detail_value if attribute == "organizer_email" else "same@example.com",
        transcript_url=detail_value if attribute == "transcript_url" else "https://example.test/same",
        slug=detail_value if attribute == "slug" else "same",
    )
    assert getattr(resolve_meeting(_evidence(listed, _detail(detailed_meeting))).meeting, attribute) == list_value
    assert getattr(resolve_meeting(_evidence(None, _detail(detailed_meeting))).meeting, attribute) == detail_value


def test_participants_always_use_list_even_when_empty_and_detail_is_fallback() -> None:
    detail = _detail(_meeting(participants=["detail@example.com"]))
    assert resolve_meeting(_evidence(_meeting(participants=[]), detail)).meeting.participants == []
    assert resolve_meeting(_evidence(None, detail)).meeting.participants == ["detail@example.com"]


def test_summary_status_uses_detail_then_list_then_age_out() -> None:
    assert (
        resolve_meeting(
            _evidence(
                _meeting(summary_status="processed"),
                _detail(_meeting(summary_status="skipped")),
            )
        ).meeting.meeting_info.summary_status
        == "skipped"
    )
    assert (
        resolve_meeting(
            _evidence(
                _meeting(summary_status="processed"),
                _detail(_meeting(summary_status="")),
            )
        ).meeting.meeting_info.summary_status
        == "processed"
    )
    old = _meeting(date_epoch_ms=_NOW_MS - 13 * 60 * 60 * 1000, duration_mins=0)
    aged = resolve_meeting(_evidence(old, _detail(_meeting(duration_mins=0))))
    fresh = resolve_meeting(
        _evidence(
            old,
            _detail(_meeting(duration_mins=0)),
            now_ms=old.date_epoch_ms + 60 * 60 * 1000,
        )
    )
    assert aged.meeting.meeting_info.summary_status == "missing_from_api"
    assert aged.provenance["meeting_info.summary_status"] == "synthetic-age-out"
    assert fresh.meeting.meeting_info.summary_status == ""


def test_summary_status_is_monotonic_but_missing_from_api_can_revive() -> None:
    processed = _detail(_meeting(summary_status="processed"))
    processing = _detail(_meeting(summary_status="processing"))
    assert merge_detail_observation(processed, processing).meeting.meeting_info.summary_status == "processed"
    missing = _detail(_meeting(summary_status="missing_from_api"))
    revived = _detail(_meeting(summary_status="processed"))
    assert merge_detail_observation(missing, revived).meeting.meeting_info.summary_status == "processed"


def test_detail_meeting_info_wins_and_list_is_fallback() -> None:
    listed = _meeting(fred_joined=False, silent_meeting=False)
    detail = _detail(_meeting(fred_joined=True, silent_meeting=True))
    resolved = resolve_meeting(_evidence(listed, detail)).meeting.meeting_info
    fallback = resolve_meeting(_evidence(listed, None)).meeting.meeting_info
    assert resolved.fred_joined and resolved.silent_meeting
    assert not fallback.fred_joined and not fallback.silent_meeting


def test_live_sentences_merge_by_index_but_terminal_detail_is_final() -> None:
    original = _sentence(1, "detail")
    correction = _sentence(1, "live correction")
    addition = _sentence(2, "live addition")
    detail = _detail(_meeting(is_live=False), sentences=[original])
    live = resolve_meeting(
        _evidence(
            _meeting(is_live=True),
            detail,
            live_captions={"1": correction, "socket-id-2": addition},
        )
    )
    assert [sentence.text for sentence in live.sentences] == ["live correction", "live addition"]
    assert live.state == "live"

    terminal_detail = _detail(_meeting(summary_status="processed"), sentences=[original])
    terminal = resolve_meeting(
        _evidence(
            _meeting(is_live=True),
            terminal_detail,
            access_logs=access_logs_ok([]),
            live_captions={"1": correction, "2": addition},
        )
    )
    assert terminal.sentences == (original,)
    assert terminal.state == "captured"


def test_access_log_outcomes_distinguish_empty_success_pending_and_failure() -> None:
    terminal = _detail(_meeting(summary_status="processed"))
    ok = resolve_meeting(_evidence(_meeting(), terminal, access_logs=access_logs_ok([])))
    pending = resolve_meeting(_evidence(_meeting(), terminal, access_logs=ACCESS_LOGS_PENDING))
    failed = resolve_meeting(_evidence(_meeting(), terminal, access_logs=ACCESS_LOGS_FAILED))
    assert ok.state == "captured" and ok.access_logs == ()
    assert pending.state == "partial"
    assert failed.state == "partial"


def test_no_detail_and_nonlive_detail_are_partial() -> None:
    assert resolve_meeting(_evidence(_meeting(), None)).state == "partial"
    assert resolve_meeting(_evidence(_meeting(), _detail())).state == "partial"


@dataclass(frozen=True)
class _CorpusExpectation:
    meeting_id: str
    value: Callable[[ResolvedMeeting], object]
    expected: object


_CORPUS_CASES = (
    _CorpusExpectation("01KV9E2A67XNMESD5AKP5KATSH", lambda item: item.meeting.duration_mins, 60.0),
    _CorpusExpectation("01KRYRZ3XSDW563YA8G7ZBEW33", lambda item: item.meeting.duration_mins, 11.640000343322754),
    _CorpusExpectation("01KFKFT8F5E88FYQ6QR9R1TXXA", lambda item: item.meeting.is_live, False),
    _CorpusExpectation("01KX4N9PA70E4M874RDG13X92P", lambda item: item.meeting.is_live, False),
    _CorpusExpectation(
        "01KEJ5VXZVD9D5A0EM59C6YA96",
        lambda item: item.meeting.meeting_info.summary_status,
        "missing_from_api",
    ),
    _CorpusExpectation(
        "01KV9E2A67XNMESD5AKP5KATSH",
        lambda item: item.meeting.meeting_info.summary_status,
        "missing_from_api",
    ),
    _CorpusExpectation(
        "01KRYRZ3XSDW563YA8G7ZBEW33",
        lambda item: item.meeting.meeting_info.summary_status,
        "skipped",
    ),
    _CorpusExpectation(
        "01KY5KYMJV24A6JXE283MJ3SGM",
        lambda item: item.meeting.title,
        "7/22 - Allyson & Todd Terrazas (FBRC.ai / AI On The Lot / LA Comfy meetups)",
    ),
    _CorpusExpectation("01KS8VAT08HQXHX43CXV6WP6K5", lambda item: item.meeting.date_epoch_ms, 1779487295073.0),
    _CorpusExpectation("01KNSM06V695H1PCP3YXKSECHJ", lambda item: item.meeting.date_str, "2026-04-15"),
)


def _load_corpus(meeting_id: str) -> MeetingEvidence:
    raw: object = json.loads((_FIXTURE_DIR / f"{meeting_id}.json").read_text())
    assert isinstance(raw, dict)
    fixture = cast("dict[str, object]", raw)
    listed = Meeting.model_validate(fixture["list_meeting"])
    detail = TranscriptDetail.model_validate(fixture["detail"])
    raw_logs = fixture.get("access_logs")
    entries: list[AccessLogEntry] = []
    if isinstance(raw_logs, list):
        for row in cast("list[object]", raw_logs):
            entries.append(AccessLogEntry.model_validate(row))
    return _evidence(listed, detail, access_logs=access_logs_ok(entries), now_ms=_NOW_MS)


@pytest.mark.parametrize("case", _CORPUS_CASES, ids=lambda case: case.meeting_id)
def test_audited_resolver_corpus(case: _CorpusExpectation) -> None:
    first = resolve_meeting(_load_corpus(case.meeting_id))
    second = resolve_meeting(_load_corpus(case.meeting_id))
    assert first == second
    assert case.value(first) == case.expected
