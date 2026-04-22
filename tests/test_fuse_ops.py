"""Focused tests for FUSE read behavior around live transcript caching."""

from __future__ import annotations

import os
from pathlib import Path
from typing import cast

import pyfuse3
import trio

from fireflies_meetings.api import FirefliesClient
from fireflies_meetings.fuse_ops import FirefliesMeetingOps
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

    def list_transcripts(self, *, max_pages: int | None = None) -> list[Meeting]:
        return []


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


def test_dynamic_read_uses_cached_live_bytes(tmp_path: Path) -> None:
    status_cache = StatusCache(cache_dir=tmp_path / "cache" / "completed")
    meeting = _make_live_meeting()
    detail = TranscriptDetail(meeting=meeting)
    client = _FakeClient(detail)
    store = MeetingStore(cast(FirefliesClient, client), status_cache=status_cache)

    assert store.watch_meeting(meeting.id)
    assert client.calls == 1
    store.mark_list_cache_fresh()

    ops = FirefliesMeetingOps(store)

    async def _exercise() -> None:
        inode = pyfuse3.ROOT_INODE
        for segment in (
            meeting.date_str[:7],
            meeting.date_str[8:10],
            meeting.slug,
            "transcript.md",
        ):
            attr = await ops.lookup(inode, segment.encode(), cast(pyfuse3.RequestContext, None))
            inode = attr.st_ino

        fi = await ops.open(inode, os.O_RDONLY, cast(pyfuse3.RequestContext, None))
        first = await ops.read(fi.fh, 0, 4096)
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

        second = await ops.read(fi.fh, 0, 4096)
        assert client.calls == 1
        assert b"Fresh live row." in second
        assert len(second) >= len(first)

        await ops.release(fi.fh)

    trio.run(_exercise)
