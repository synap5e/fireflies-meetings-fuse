from __future__ import annotations

import json
from pathlib import Path

from fireflies_meetings.capture import CaptureStore
from fireflies_meetings.models import AccessLogEntry, Meeting, MeetingInfo, Sentence, TranscriptDetail
from fireflies_meetings.projection import Projection, ProjectionBuildOptions, build_projection_from_captures


def _meeting(
    meeting_id: str,
    *,
    duration_mins: float,
    summary_status: str = "processed",
    epoch_offset_ms: float = 0.0,
    organizer_email: str = "alice@example.com",
) -> Meeting:
    return Meeting(
        id=meeting_id,
        title="Simon Luke",
        date_epoch_ms=1774891800000.0 + epoch_offset_ms,
        duration_mins=duration_mins,
        organizer_email=organizer_email,
        participants=[organizer_email],
        transcript_url=f"https://app.fireflies.ai/view/{meeting_id}",
        meeting_info=MeetingInfo(summary_status=summary_status),
    )


def _sentence(index: int, text: str, *, speaker: str = "Alice") -> Sentence:
    start = float(index * 2 + 1)
    return Sentence(
        index=index,
        text=text,
        start_time=start,
        end_time=start + 1,
        speaker_name=speaker,
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


def _build_projection(
    tmp_path: Path,
    meetings: list[Meeting],
    *,
    user_email: str | None = None,
    sentences_by_id: dict[str, list[Sentence]] | None = None,
) -> Projection:
    capture = CaptureStore(tmp_path)
    capture.write_list(meetings, fetched_at=1.0)
    sentences = sentences_by_id or {}
    for meeting in meetings:
        capture.write_detail(
            meeting.id,
            TranscriptDetail(
                meeting=meeting,
                sentences=sentences.get(meeting.id, [_sentence(0, f"{meeting.id} sentence.")]),
            ),
        )
        capture.write_access_logs(meeting.id, [_access_log()])
    return build_projection_from_captures(
        capture.read_snapshot(),
        ProjectionBuildOptions(user_email=user_email),
    )


def test_zero_duration_completed_collision_folds_to_ghost(tmp_path: Path) -> None:
    real = _meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0)
    ghost = _meeting("GHOST01", duration_mins=0.0)

    projection = _build_projection(tmp_path, [ghost, real])

    assert projection.meetings["REAL01"].ghost_id == "GHOST01"
    assert projection.meetings["GHOST01"].primary_path is None
    assert projection.node("/2026-03/31/simon-luke/ghost/meeting.json") is not None
    assert projection.node("/2026-03/31/simon-luke-2") is None


def test_non_terminal_zero_duration_collision_does_not_fold(tmp_path: Path) -> None:
    real = _meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0)
    candidate = _meeting("GHOST01", duration_mins=0.0, summary_status="")

    projection = _build_projection(tmp_path, [candidate, real])

    assert projection.meetings["REAL01"].ghost_id is None
    assert projection.meetings["GHOST01"].primary_path == "/2026-03/31/simon-luke"
    assert projection.meetings["REAL01"].primary_path == "/2026-03/31/simon-luke-2"


def test_three_way_collision_does_not_fold_ghost(tmp_path: Path) -> None:
    real = _meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0)
    ghost = _meeting("GHOST01", duration_mins=0.0)
    reconnect = _meeting("REAL02", duration_mins=5.0, epoch_offset_ms=2000.0)

    projection = _build_projection(tmp_path, [ghost, real, reconnect])

    assert projection.meetings["GHOST01"].primary_path == "/2026-03/31/simon-luke"
    assert projection.meetings["REAL01"].primary_path == "/2026-03/31/simon-luke-2"
    assert projection.meetings["REAL02"].primary_path == "/2026-03/31/simon-luke-3"
    assert projection.meetings["REAL01"].ghost_id is None


def test_overlap_folding_exposes_warning_and_overlap_dir(tmp_path: Path) -> None:
    primary = _meeting("PRIMARY01", duration_mins=60.0)
    overlap = _meeting("OVERLAP01", duration_mins=10.0, epoch_offset_ms=5 * 60_000.0)
    separate = _meeting("SEPARATE01", duration_mins=15.0, epoch_offset_ms=90 * 60_000.0)

    projection = _build_projection(tmp_path, [separate, overlap, primary])

    assert projection.meetings["PRIMARY01"].overlap_ids == ("OVERLAP01",)
    assert projection.meetings["OVERLAP01"].primary_path is None
    assert projection.meetings["SEPARATE01"].primary_path == "/2026-03/31/simon-luke-2"
    assert projection.node("/2026-03/31/simon-luke/_overlap_warning.md") is not None
    assert projection.node("/2026-03/31/simon-luke/overlap/meeting.json") is not None


def test_overlap_warning_reports_missing_sentences(tmp_path: Path) -> None:
    primary = _meeting("PRIMARY01", duration_mins=60.0)
    overlap = _meeting("OVERLAP01", duration_mins=10.0, epoch_offset_ms=5 * 60_000.0)

    projection = _build_projection(
        tmp_path,
        [primary, overlap],
        sentences_by_id={
            "PRIMARY01": [
                _sentence(0, "Shared sentence."),
                _sentence(1, "Primary only."),
            ],
            "OVERLAP01": [
                _sentence(0, " shared sentence. "),
                _sentence(1, "Overlap only detail.", speaker="Bob"),
            ],
        },
    )

    warning = projection.file_content("/2026-03/31/simon-luke/_overlap_warning.md")
    assert warning is not None
    text = warning.decode()
    assert "warning: overlap-not-superset" in text
    assert "NOT a strict superset" in text
    assert "## From `overlap/` (ID: OVERLAP01)" in text
    assert "| 0:03 | Bob | Overlap only detail. |" in text
    assert "shared sentence" not in text.lower()


def test_overlap_warning_reports_superset(tmp_path: Path) -> None:
    primary = _meeting("PRIMARY01", duration_mins=60.0)
    overlap = _meeting("OVERLAP01", duration_mins=10.0, epoch_offset_ms=5 * 60_000.0)

    projection = _build_projection(
        tmp_path,
        [primary, overlap],
        sentences_by_id={
            "PRIMARY01": [
                _sentence(0, "Shared sentence."),
                _sentence(1, "Overlap only detail.", speaker="Bob"),
            ],
            "OVERLAP01": [
                _sentence(0, " shared sentence. "),
                _sentence(1, "overlap only detail.", speaker="Bob"),
            ],
        },
    )

    warning = projection.file_content("/2026-03/31/simon-luke/_overlap_warning.md")
    assert warning is not None
    text = warning.decode()
    assert "warning: overlap-superset" in text
    assert "No missing sentences were found" in text
    assert "NOT a strict superset" not in text


def test_mine_listing_uses_full_date_ghost_context(tmp_path: Path) -> None:
    real = _meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0)
    ghost = _meeting(
        "GHOST01",
        duration_mins=0.0,
        organizer_email="calendar@example.com",
    )

    projection = _build_projection(tmp_path, [ghost, real], user_email="alice@example.com")

    assert projection.meetings["REAL01"].mine_path == "/mine/2026-03/31/simon-luke"
    assert projection.meetings["GHOST01"].mine_path is None
    assert projection.node("/mine/2026-03/31/simon-luke/ghost/meeting.json") is not None
    meeting_json = projection.file_content("/mine/2026-03/31/simon-luke/meeting.json")
    assert meeting_json is not None
    assert json.loads(meeting_json)["id"] == "REAL01"
