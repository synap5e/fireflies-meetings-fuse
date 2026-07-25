"""Tests for internal live transcript stream helpers."""

from __future__ import annotations

import queue
import threading
import time
from pathlib import Path
from typing import cast

from fireflies_meetings.api import FirefliesClient
from fireflies_meetings.live_stream import (
    make_broadcast_handler,
    normalize_stream_sentence,
    shutdown_drainer,
    spawn_caption_drainer,
)
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


def test_broadcast_handler_returns_immediately_when_drainer_is_slow() -> None:
    """The socketio handler runs in an engineio-spawned thread — if it blocks
    on on_update, that thread lives until on_update returns, and further
    incoming messages spawn more threads that pile up (7k+ zombie threads
    observed in production). Handler must enqueue and return under a
    millisecond even if the drainer is stuck."""
    caption_queue: queue.Queue[tuple[str, Sentence] | None] = queue.Queue(maxsize=8)
    handler = make_broadcast_handler("MEET01", caption_queue)

    raw = {
        "transcript_id": "1",
        "sentence": "hello",
        "speaker_name": "A",
        "time": 1.0,
        "endTime": 2.0,
    }
    start = time.perf_counter()
    for _ in range(100):
        handler(raw)
    elapsed = time.perf_counter() - start
    assert elapsed < 0.5, f"handler blocked for {elapsed:.3f}s across 100 calls"


def test_broadcast_handler_drops_when_queue_full_without_blocking() -> None:
    """When the drainer can't keep up, over-cap captions must be dropped, not
    block the handler — otherwise the leak this refactor prevents comes back."""
    caption_queue: queue.Queue[tuple[str, Sentence] | None] = queue.Queue(maxsize=2)
    handler = make_broadcast_handler("MEET01", caption_queue)

    raw = {
        "transcript_id": "1",
        "sentence": "hello",
        "speaker_name": "A",
        "time": 1.0,
        "endTime": 2.0,
    }
    for _ in range(10):
        handler(raw)  # fills 2, drops 8; must not block
    assert caption_queue.qsize() == 2


def test_caption_drainer_delivers_and_shuts_down_cleanly() -> None:
    caption_queue: queue.Queue[tuple[str, Sentence] | None] = queue.Queue(maxsize=8)
    received: list[tuple[str, Sentence]] = []
    lock = threading.Lock()

    def on_update(transcript_id: str, sentence: Sentence) -> None:
        with lock:
            received.append((transcript_id, sentence))

    drainer = spawn_caption_drainer("MEET01", caption_queue, on_update)
    for i in range(5):
        sentence = Sentence(index=i, text=f"t{i}", start_time=float(i), end_time=float(i + 1))
        caption_queue.put((f"row-{i}", sentence))
    shutdown_drainer(caption_queue, drainer)

    assert not drainer.is_alive()
    assert [tid for tid, _ in received] == [f"row-{i}" for i in range(5)]


def test_stream_update_replaces_same_row(tmp_path: Path) -> None:
    status_cache = StatusCache(cache_dir=tmp_path / "cache")
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
    status_cache = StatusCache(cache_dir=tmp_path / "cache")
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
    status_cache = StatusCache(cache_dir=tmp_path / "cache")
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
) -> None:
    status_cache = StatusCache(cache_dir=tmp_path / "cache")
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

    content, completed = store.get_file(meeting.id, "transcript.md")
    assert content is not None
    assert not completed
    assert client.calls == 1
