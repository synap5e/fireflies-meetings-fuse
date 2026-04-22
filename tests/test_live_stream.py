"""Tests for internal live transcript stream helpers."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from fireflies_meetings.api import FirefliesClient
from fireflies_meetings.live_stream import normalize_stream_sentence
from fireflies_meetings.models import Meeting, MeetingInfo, Sentence, TranscriptDetail
from fireflies_meetings.status_cache import StatusCache
from fireflies_meetings.store import MeetingStore


class _FakeClient:
    def __init__(self, detail: TranscriptDetail) -> None:
        self._detail = detail
        self.calls = 0

    def get_transcript(self, meeting_id: str) -> TranscriptDetail:
        assert meeting_id == self._detail.meeting.id
        self.calls += 1
        return self._detail


def _make_live_meeting() -> Meeting:
    return Meeting(
        id="MEET01",
        title="Live Standup",
        date_epoch_ms=1774891800000.0,
        date_str="2026-03-31",
        is_live=True,
        organizer_email="alice@example.com",
        participants=["alice@example.com"],
        transcript_url="https://app.fireflies.ai/view/MEET01",
        meeting_info=MeetingInfo(summary_status=""),
        slug="live-standup",
    )


def test_normalize_stream_sentence_parses_fireflies_event() -> None:
    normalized = normalize_stream_sentence({
        "transcript_id": "65156",
        "sentence": "Did you want some more events to be sent?",
        "speaker_name": "Simon Pinfold",
        "time": 5547.373,
        "endTime": 5549.593,
    })

    assert normalized is not None
    transcript_id, sentence = normalized
    assert transcript_id == "65156"
    assert sentence.index == 65156
    assert sentence.text == "Did you want some more events to be sent?"
    assert sentence.speaker_name == "Simon Pinfold"


def test_stream_update_replaces_same_row(tmp_path: Path) -> None:
    status_cache = StatusCache(cache_dir=tmp_path / "cache" / "completed")
    meeting = _make_live_meeting()
    detail = TranscriptDetail(meeting=meeting)
    client = _FakeClient(detail)
    store = MeetingStore(cast(FirefliesClient, client), status_cache=status_cache)

    assert store.watch_meeting(meeting.id)

    store.apply_live_transcript_update(
        meeting.id,
        "65156",
        Sentence(
            index=65156,
            text="Did you want",
            start_time=5547.373,
            end_time=5548.0,
            speaker_name="Simon Pinfold",
        ),
    )
    store.apply_live_transcript_update(
        meeting.id,
        "65156",
        Sentence(
            index=65156,
            text="Did you want some more events to be sent?",
            start_time=5547.373,
            end_time=5549.593,
            speaker_name="Simon Pinfold",
        ),
    )

    content, completed = store.get_file(meeting.id, "transcript.md")

    assert content is not None
    assert not completed
    text = content.decode()
    assert "Did you want some more events to be sent?" in text
    assert "Did you want\n" not in text


def test_stream_update_replaces_same_nonnumeric_row(tmp_path: Path) -> None:
    status_cache = StatusCache(cache_dir=tmp_path / "cache" / "completed")
    meeting = _make_live_meeting()
    detail = TranscriptDetail(meeting=meeting)
    store = MeetingStore(
        cast(FirefliesClient, _FakeClient(detail)),
        status_cache=status_cache,
    )

    assert store.watch_meeting(meeting.id)

    store.apply_live_transcript_update(
        meeting.id,
        "row-abc",
        Sentence(
            index=0,
            text="First draft",
            start_time=5.0,
            end_time=6.0,
            speaker_name="Simon Pinfold",
        ),
    )
    store.apply_live_transcript_update(
        meeting.id,
        "row-abc",
        Sentence(
            index=0,
            text="Corrected final draft",
            start_time=5.0,
            end_time=6.0,
            speaker_name="Simon Pinfold",
        ),
    )

    content, completed = store.get_file(meeting.id, "transcript.md")

    assert content is not None
    assert not completed
    text = content.decode()
    assert "Corrected final draft" in text
    assert "First draft" not in text


def test_stream_update_preserves_api_baseline(tmp_path: Path) -> None:
    status_cache = StatusCache(cache_dir=tmp_path / "cache" / "completed")
    meeting = _make_live_meeting()
    detail = TranscriptDetail(
        meeting=meeting,
        sentences=[
            Sentence(
                index=1,
                text="Baseline API sentence.",
                start_time=1.0,
                end_time=2.0,
                speaker_name="Alice",
            ),
        ],
    )
    store = MeetingStore(
        cast(FirefliesClient, _FakeClient(detail)),
        status_cache=status_cache,
    )

    assert store.watch_meeting(meeting.id)

    store.apply_live_transcript_update(
        meeting.id,
        "65156",
        Sentence(
            index=65156,
            text="Live stream sentence.",
            start_time=5.0,
            end_time=6.0,
            speaker_name="Bob",
        ),
    )

    content, completed = store.get_file(meeting.id, "transcript.md")

    assert content is not None
    assert not completed
    text = content.decode()
    assert "Baseline API sentence." in text
    assert "Live stream sentence." in text


def test_live_cache_reused_until_detail_ttl_expires(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status_cache = StatusCache(cache_dir=tmp_path / "cache" / "completed")
    meeting = _make_live_meeting()
    detail = TranscriptDetail(meeting=meeting)
    client = _FakeClient(detail)
    store = MeetingStore(cast(FirefliesClient, client), status_cache=status_cache)

    assert store.watch_meeting(meeting.id)
    assert client.calls == 1

    content, completed = store.get_file(meeting.id, "transcript.md")
    assert content is not None
    assert not completed
    assert client.calls == 1

    store.apply_live_transcript_update(
        meeting.id,
        "65156",
        Sentence(
            index=65156,
            text="Fresh live row.",
            start_time=5.0,
            end_time=6.0,
            speaker_name="Alice",
        ),
    )

    content, completed = store.get_file(meeting.id, "transcript.md")
    assert content is not None
    assert not completed
    assert client.calls == 1

    monkeypatch.setattr("fireflies_meetings.store._DETAIL_TTL", 0.0)
    content, completed = store.get_file(meeting.id, "transcript.md")
    assert content is not None
    assert not completed
    assert client.calls == 2
