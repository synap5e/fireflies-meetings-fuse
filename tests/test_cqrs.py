from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

from fireflies_meetings import store as store_module
from fireflies_meetings.access_logs import (
    ACCESS_LOGS_FAILED,
    AccessLogsFailed,
    AccessLogsOk,
    AccessLogsOutcome,
    access_logs_ok,
)
from fireflies_meetings.api import FirefliesClient
from fireflies_meetings.capture import CaptureStore, migrate_legacy_cache
from fireflies_meetings.commands import (
    AccessLogsFetched,
    CommandProcessor,
    DetailFetched,
    ListRefreshed,
    LiveCaptionArrived,
    StatusSupplemented,
)
from fireflies_meetings.inode_map import InodeMap
from fireflies_meetings.models import (
    AccessLogEntry,
    Meeting,
    MeetingAttendee,
    MeetingInfo,
    Sentence,
    Speaker,
    Summary,
    TranscriptDetail,
)
from fireflies_meetings.projection import build_projection_from_captures
from fireflies_meetings.status_cache import StatusCache
from fireflies_meetings.store import MeetingStore


class _DetailClient:
    def __init__(self, detail: TranscriptDetail) -> None:
        self._detail = detail

    def get_transcript(self, meeting_id: str) -> TranscriptDetail:
        assert meeting_id == self._detail.meeting.id
        return self._detail

    def list_transcripts(self, *, max_pages: int | None = None) -> list[Meeting]:
        return []

    def list_recent_status_meetings(self, *, limit: int = 100) -> list[Meeting]:
        return []

    def get_access_logs(self, meeting_id: str) -> AccessLogsOutcome:
        return access_logs_ok([])


def _meeting(
    meeting_id: str = "MEET01",
    *,
    title: str = "Planning Sync",
    summary_status: str = "processed",
    is_live: bool = False,
    duration_mins: float = 30.0,
) -> Meeting:
    return Meeting(
        id=meeting_id,
        title=title,
        date_epoch_ms=1774891800000.0,
        duration_mins=duration_mins,
        is_live=is_live,
        organizer_email="alice@example.com",
        participants=["alice@example.com", "bob@example.com"],
        transcript_url=f"https://app.fireflies.ai/view/{meeting_id}",
        meeting_info=MeetingInfo(summary_status=summary_status),
        slug="planning-sync" if title == "Planning Sync" else "",
    )


def _sentence(index: int = 1, text: str = "Hello from the transcript.") -> Sentence:
    return Sentence(
        index=index,
        text=text,
        start_time=float(index),
        end_time=float(index + 1),
        speaker_name="Alice",
    )


def _detail(meeting: Meeting | None = None) -> TranscriptDetail:
    target = meeting or _meeting()
    return TranscriptDetail(
        meeting=target,
        sentences=[_sentence()],
        speakers=[Speaker(id=1, name="Alice")],
        summary=Summary(short_summary="A concise summary."),
        attendees=[MeetingAttendee(display_name="Alice", email="alice@example.com")],
    )


def _access_log() -> AccessLogEntry:
    return AccessLogEntry(
        id="LOG01",
        user_id="U1",
        user_email="viewer@example.com",
        user_name="Viewer",
        action="view_summary",
        timestamp="2026-03-31T12:00:00Z",
    )


def test_projection_build_is_idempotent(tmp_path: Path) -> None:
    capture = CaptureStore(tmp_path)
    meeting = _meeting()
    capture.write_list([meeting], fetched_at=1.0)
    capture.write_detail(meeting.id, _detail(meeting))
    capture.write_access_logs(meeting.id, access_logs_ok([_access_log()]))

    first = build_projection_from_captures(capture.read_snapshot())
    second = build_projection_from_captures(capture.read_snapshot())

    assert first.nodes == second.nodes
    assert first.meetings == second.meetings


def test_commands_update_projection_and_capture_files(tmp_path: Path) -> None:
    capture = CaptureStore(tmp_path)
    processor = CommandProcessor(capture)
    meeting = _meeting(summary_status="processing")

    processor.apply(ListRefreshed(name="list-refreshed", meetings=[meeting]), fetched_at=1.0)
    assert processor.projection.meetings[meeting.id].capture_state == "partial"
    assert (tmp_path / "list.json").is_file()

    detail = _detail(meeting.model_copy(update={"meeting_info": MeetingInfo(summary_status="processed")}))
    processor.apply(DetailFetched(name="detail-fetched", meeting_id=meeting.id, detail=detail), fetched_at=2.0)
    assert (tmp_path / "meetings" / meeting.id / "detail.json").is_file()

    processor.apply(
        AccessLogsFetched(
            name="access-logs-fetched",
            meeting_id=meeting.id,
            outcome=access_logs_ok([_access_log()]),
        ),
        fetched_at=3.0,
    )
    assert processor.projection.meetings[meeting.id].capture_state == "captured"
    assert b"Viewer" in processor.projection.meetings[meeting.id].files["views.md"]


def test_command_edges_unknown_detail_status_supplement_and_late_live(tmp_path: Path) -> None:
    capture = CaptureStore(tmp_path)
    processor = CommandProcessor(capture)
    unknown = _meeting("UNKNOWN01", title="Unknown Detail")
    processor.apply(
        DetailFetched(name="detail-fetched", meeting_id=unknown.id, detail=_detail(unknown)),
        fetched_at=1.0,
    )
    assert "UNKNOWN01" in processor.projection.meetings

    supplement = _meeting("SUPP01", title="Supplemented", summary_status="processing")
    processor.apply(StatusSupplemented(name="status-supplemented", meetings=[supplement]), fetched_at=2.0)
    processor.apply(StatusSupplemented(name="status-supplemented", meetings=[supplement]), fetched_at=3.0)
    assert list(capture.read_list()).count(supplement) == 1

    captured = _meeting("DONE01")
    processor.apply(ListRefreshed(name="list-refreshed", meetings=[captured]), fetched_at=4.0)
    processor.apply(
        DetailFetched(name="detail-fetched", meeting_id=captured.id, detail=_detail(captured)),
        fetched_at=5.0,
    )
    processor.apply(
        AccessLogsFetched(
            name="access-logs-fetched",
            meeting_id=captured.id,
            outcome=access_logs_ok([_access_log()]),
        ),
        fetched_at=6.0,
    )
    before = processor.projection.meetings[captured.id].files["transcript.md"]
    processor.apply(
        LiveCaptionArrived(name="live-caption-arrived", meeting_id=captured.id, sentence=_sentence(99, "Late row")),
        fetched_at=7.0,
    )
    assert processor.projection.meetings[captured.id].files["transcript.md"] == before


def test_live_caption_command_renders_partial_transcript(tmp_path: Path) -> None:
    capture = CaptureStore(tmp_path)
    processor = CommandProcessor(capture)
    live = _meeting("LIVE01", summary_status="processing", is_live=True)
    processor.apply(ListRefreshed(name="list-refreshed", meetings=[live]), fetched_at=1.0)
    processor.apply(
        LiveCaptionArrived(name="live-caption-arrived", meeting_id=live.id, sentence=_sentence(2, "Live caption")),
        fetched_at=2.0,
    )
    projected = processor.projection.meetings[live.id]
    assert projected.capture_state == "live"
    assert b"Live caption" in projected.files["transcript.md"]


def test_late_caption_cannot_taint_terminal_meeting(tmp_path: Path) -> None:
    """DetailFetched(terminal) clears the meeting's captions and marks it
    off-limits. AccessLogsFetched arrives later. In the window between them
    the projection state is still 'partial' — a queued caption that arrives
    there was silently re-injected into _live_captions (only 'captured' was
    rejected) and merged into the terminal transcript by _project_detail.
    The guard now keys off the terminal-detail set so late captions are
    dropped regardless of arrival order."""
    capture = CaptureStore(tmp_path)
    processor = CommandProcessor(capture)
    live = _meeting("MEET01", summary_status="processing", is_live=True)
    processor.apply(ListRefreshed(name="list-refreshed", meetings=[live]), fetched_at=1.0)
    processor.apply(
        LiveCaptionArrived(name="live-caption-arrived", meeting_id=live.id, sentence=_sentence(1, "Real caption")),
        fetched_at=2.0,
    )

    terminal = live.model_copy(update={
        "is_live": False,
        "meeting_info": MeetingInfo(summary_status="processed"),
    })
    processor.apply(
        DetailFetched(name="detail-fetched", meeting_id=live.id, detail=_detail(terminal)),
        fetched_at=3.0,
    )
    # Terminal detail is written; access logs haven't arrived yet, so state
    # is 'partial'. A late caption sneaks in here — must be rejected.
    assert processor.projection.meetings[live.id].capture_state == "partial"
    processor.apply(
        LiveCaptionArrived(name="live-caption-arrived", meeting_id=live.id, sentence=_sentence(99, "LATE STALE ROW")),
        fetched_at=4.0,
    )
    processor.apply(
        AccessLogsFetched(
            name="access-logs-fetched",
            meeting_id=live.id,
            outcome=access_logs_ok([_access_log()]),
        ),
        fetched_at=5.0,
    )

    projected = processor.projection.meetings[live.id]
    assert projected.capture_state == "captured"
    assert b"LATE STALE ROW" not in projected.files["transcript.md"]


def test_terminal_meeting_ids_survive_processor_reboot(tmp_path: Path) -> None:
    """After restart, the in-memory terminal-ids set is empty. It must be
    seeded from on-disk terminal details so the late-caption guard still
    holds across the process boundary."""
    capture = CaptureStore(tmp_path)
    processor = CommandProcessor(capture)
    live = _meeting("REBOOT01", summary_status="processed", is_live=False)
    processor.apply(ListRefreshed(name="list-refreshed", meetings=[live]), fetched_at=1.0)
    processor.apply(
        DetailFetched(name="detail-fetched", meeting_id=live.id, detail=_detail(live)),
        fetched_at=2.0,
    )

    reborn = CommandProcessor(capture)
    reborn.apply(
        LiveCaptionArrived(name="live-caption-arrived", meeting_id=live.id, sentence=_sentence(1, "GHOST CAPTION")),
        fetched_at=3.0,
    )
    reborn.apply(
        AccessLogsFetched(
            name="access-logs-fetched",
            meeting_id=live.id,
            outcome=access_logs_ok([_access_log()]),
        ),
        fetched_at=4.0,
    )
    projected = reborn.projection.meetings[live.id]
    assert projected.capture_state == "captured"
    assert b"GHOST CAPTION" not in projected.files["transcript.md"]


def test_detail_terminal_still_is_live_captures_with_logs(tmp_path: Path) -> None:
    """Even when the detail keeps is_live=True (a bug on Fireflies' side we've
    observed on old meetings), a terminal summary_status + access logs is
    enough evidence to promote past 'partial'. Previously _capture_state
    required Meeting.is_completed, which is False whenever is_live=True — so
    such meetings sat in the backfill queue forever."""
    capture = CaptureStore(tmp_path)
    processor = CommandProcessor(capture)
    stuck_live = _meeting("STUCKLIVE01", summary_status="processed", is_live=True)
    processor.apply(ListRefreshed(name="list-refreshed", meetings=[stuck_live]), fetched_at=1.0)
    processor.apply(
        DetailFetched(name="detail-fetched", meeting_id=stuck_live.id, detail=_detail(stuck_live)),
        fetched_at=2.0,
    )
    processor.apply(
        AccessLogsFetched(
            name="access-logs-fetched",
            meeting_id=stuck_live.id,
            outcome=access_logs_ok([_access_log()]),
        ),
        fetched_at=3.0,
    )
    projected = processor.projection.meetings[stuck_live.id]
    assert projected.capture_state == "captured"


def test_stale_is_live_transitions_out_when_detail_summary_terminal(tmp_path: Path) -> None:
    """List meetings can carry a stale is_live=True (the list API never returns
    a terminal summary_status to clear it). A subsequently-fetched detail with
    a terminal status must promote the meeting past capture_state="live" so
    summary.md renders instead of staying stuck on "Summary pending"."""
    capture = CaptureStore(tmp_path)
    processor = CommandProcessor(capture)
    list_meeting = _meeting("STALE01", summary_status="", is_live=True)
    processor.apply(ListRefreshed(name="list-refreshed", meetings=[list_meeting]), fetched_at=1.0)
    assert processor.projection.meetings[list_meeting.id].capture_state == "live"

    completed = list_meeting.model_copy(update={
        "is_live": False,
        "meeting_info": MeetingInfo(summary_status="processed"),
    })
    processor.apply(
        DetailFetched(name="detail-fetched", meeting_id=list_meeting.id, detail=_detail(completed)),
        fetched_at=2.0,
    )
    processor.apply(
        AccessLogsFetched(
            name="access-logs-fetched",
            meeting_id=list_meeting.id,
            outcome=access_logs_ok([_access_log()]),
        ),
        fetched_at=3.0,
    )

    projected = processor.projection.meetings[list_meeting.id]
    assert projected.capture_state == "captured"
    assert projected.files["summary.md"] != b"_Summary pending_\n"
    assert b"A concise summary." in projected.files["summary.md"]


def test_store_serializes_concurrent_live_caption_commands(tmp_path: Path) -> None:
    live = _meeting("LIVE99", summary_status="processing", is_live=True)
    client = _DetailClient(TranscriptDetail(meeting=live))
    store = MeetingStore(
        cast(FirefliesClient, client),
        status_cache=StatusCache(cache_dir=tmp_path / "cache"),
    )
    assert store.watch_meeting(live.id)

    def apply_row(index: int) -> None:
        store.apply_live_transcript_update(
            live.id,
            str(index),
            _sentence(index, f"Concurrent row {index}."),
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(apply_row, range(50)))

    projected = store.projection.meetings[live.id]
    transcript = projected.files["transcript.md"].decode()
    assert projected.capture_state == "live"
    for index in range(50):
        assert f"Concurrent row {index}." in transcript


def test_backfill_ages_out_empty_placeholder_after_threshold(tmp_path: Path, monkeypatch: object) -> None:
    """Fireflies leaves calendar entries in the list forever with dur=0,
    status='' when the bot never joined. Backfill fetches return 200 with
    nothing; _capture_state stays 'partial' → infinite re-poll. After the
    age threshold we promote the cached status to 'missing_from_api' so
    the meeting reaches 'captured' and drops out of the backfill queue."""
    placeholder_epoch = 1774891800000.0  # matches _meeting default
    fresh_now = placeholder_epoch / 1000.0 + 3600.0  # 1h after meeting — under threshold
    aged_now = placeholder_epoch / 1000.0 + 13 * 3600.0  # 13h after — over threshold

    placeholder = _meeting("PH01", summary_status="", duration_mins=0.0)
    empty_detail = TranscriptDetail(meeting=placeholder)

    class _SeedingClient(_DetailClient):
        def list_transcripts(self, *, max_pages: int | None = None) -> list[Meeting]:
            return [placeholder]

    client = _SeedingClient(empty_detail)
    store = MeetingStore(
        cast(FirefliesClient, client),
        status_cache=StatusCache(cache_dir=tmp_path / "cache"),
    )
    store.refresh_list_if_needed()
    assert store.projection.meetings["PH01"].capture_state == "partial"

    monkeypatch.setattr(store_module.time, "time", lambda: fresh_now)  # type: ignore[attr-defined]
    store.backfill_one("PH01")
    assert store.projection.meetings["PH01"].capture_state == "partial", "fresh placeholder should still retry"

    monkeypatch.setattr(store_module.time, "time", lambda: aged_now)  # type: ignore[attr-defined]
    store.backfill_one("PH01")
    projected = store.projection.meetings["PH01"]
    assert projected.capture_state == "captured", "aged placeholder should drop out of the backfill queue"
    assert "PH01" not in store.get_uncached_meeting_ids()


def test_inode_map_survives_projection_swap_until_forget() -> None:
    inodes = InodeMap()
    inode = inodes.get_or_create("/2026-03/31/planning-sync/transcript.md")
    assert inodes.get_or_create("/2026-03/31/planning-sync/transcript.md") == inode

    # Simulate a projection swap removing the path. The InodeMap is deliberately
    # not touched by projection swaps, so the kernel's two lookups remain valid.
    assert inodes.get_path(inode) == "/2026-03/31/planning-sync/transcript.md"
    inodes.forget(inode, 1)
    assert inodes.get_path(inode) == "/2026-03/31/planning-sync/transcript.md"
    inodes.forget(inode, 1)
    assert inodes.get_path(inode) is None


def test_migration_round_trip_preserves_structured_data(tmp_path: Path) -> None:
    detail_dir = tmp_path / "detail" / "MEET01"
    detail_dir.mkdir(parents=True)
    legacy = {
        "id": "MEET01",
        "title": "Planning Sync",
        "date": "2026-03-31",
        "date_epoch_ms": 1774891800000.0,
        "duration_mins": 30.0,
        "is_live": False,
        "organizer_email": "alice@example.com",
        "participants": ["alice@example.com"],
        "transcript_url": "https://app.fireflies.ai/view/MEET01",
        "meeting_info": {"summary_status": "missing_from_api"},
        "speakers": [{"id": 1, "name": "Alice"}],
        "attendees": [{"display_name": "Alice", "email": "alice@example.com"}],
        "summary": {"short_summary": "Stored summary"},
        "transcript": [{"index": 1, "speaker_name": "Alice", "text": "Stored text", "start_time": 1, "end_time": 2}],
        "access_logs": [
            {
                "id": "LOG01",
                "user_id": "U1",
                "user_email": "viewer@example.com",
                "user_name": "Viewer",
                "action": "view_summary",
                "timestamp": "2026-03-31T12:00:00Z",
            },
        ],
    }
    (detail_dir / "meeting.json").write_text(json.dumps(legacy))

    migrate_legacy_cache(tmp_path)

    detail = TranscriptDetail.model_validate_json((tmp_path / "meetings" / "MEET01" / "detail.json").read_text())
    logs_raw: object = json.loads((tmp_path / "meetings" / "MEET01" / "access_logs.json").read_text())
    assert isinstance(logs_raw, dict)
    typed_logs = cast("dict[str, object]", logs_raw)
    raw_log_items = typed_logs.get("logs")
    assert isinstance(raw_log_items, list)
    logs = [AccessLogEntry.model_validate(item) for item in cast("list[object]", raw_log_items)]
    assert typed_logs["outcome"] == "ok"
    assert detail.sentences[0].text == "Stored text"
    assert detail.summary is not None and detail.summary.short_summary == "Stored summary"
    assert detail.speakers[0].name == "Alice"
    assert detail.attendees[0].email == "alice@example.com"
    assert logs[0].user_email == "viewer@example.com"
    assert detail.meeting.meeting_info.summary_status == "missing_from_api"
    assert not (tmp_path / "detail").exists()
    assert list(tmp_path.glob("detail.legacy.*"))


def test_access_log_capture_reads_legacy_list_and_round_trips_tagged_outcomes(tmp_path: Path) -> None:
    capture = CaptureStore(tmp_path)
    legacy_path = capture.access_logs_path("LEGACY01")
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text(json.dumps([_access_log().model_dump()]))

    legacy = capture.read_access_logs()["LEGACY01"]
    assert isinstance(legacy, AccessLogsOk)
    assert legacy.logs == (_access_log(),)

    capture.write_access_logs("FAILED01", ACCESS_LOGS_FAILED)
    failed = capture.read_access_logs()["FAILED01"]
    assert isinstance(failed, AccessLogsFailed)
    failed_raw = json.loads(capture.access_logs_path("FAILED01").read_text())
    assert failed_raw == {"outcome": "failed"}


def test_in_progress_surface_is_minimal_and_machine_readable(tmp_path: Path) -> None:
    capture = CaptureStore(tmp_path)
    captured = _meeting("CAPTURED01")
    partial = _meeting("PARTIAL01", title="Partial", summary_status="processing")
    live = _meeting("LIVE01", title="Live", summary_status="processing", is_live=True)
    capture.write_list([captured, partial, live], fetched_at=1.0)
    capture.write_detail(captured.id, _detail(captured))
    capture.write_access_logs(captured.id, access_logs_ok([_access_log()]))

    projection = build_projection_from_captures(capture.read_snapshot())

    assert projection.meetings[partial.id].files["summary.md"] == b"_Summary pending_\n"
    assert projection.meetings[partial.id].files["transcript.md"] == b"_Transcript pending_\n"
    assert projection.meetings[partial.id].files["views.md"] == b"_Awaiting access log_\n"
    partial_json = json.loads(projection.meetings[partial.id].files["meeting.json"])
    live_json = json.loads(projection.meetings[live.id].files["meeting.json"])
    captured_json = json.loads(projection.meetings[captured.id].files["meeting.json"])
    assert partial_json["capture_state"] == "partial"
    assert live_json["capture_state"] == "live"
    assert captured_json["capture_state"] == "captured"
    assert projection.node("/BACKFILL_IN_PROGRESS") is not None
    backfill = projection.file_content("/BACKFILL_IN_PROGRESS")
    assert backfill is not None
    assert b"## 2026-03-31 partial" in backfill
    assert b"## 2026-03-31 live" in backfill
