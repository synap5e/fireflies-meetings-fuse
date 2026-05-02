# pyright: reportPrivateUsage=false
"""Tests for atomic disk persistence in MeetingStore.

Two failure modes to lock in:

- list.json must survive interrupted writes — a tmp file lying around
  must not poison the next load.
- detail/<id>/ must require a `.complete` sentinel before being treated
  as cached. A half-populated dir (interrupted mid-write) must be
  re-fetched, not served as truncated content.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from fireflies_meetings.api import FirefliesClient
from fireflies_meetings.models import Meeting, MeetingInfo, Sentence, TranscriptDetail
from fireflies_meetings.renderer import render_meeting_json
from fireflies_meetings.status_cache import StatusCache
from fireflies_meetings.store import MEETING_FILES, MeetingEntry, MeetingStore

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def cache_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    yield tmp_path / "fireflies-meetings"


@pytest.fixture
def empty_store(cache_root: Path) -> MeetingStore:
    """A store with no API client interaction — only exercises disk I/O."""
    # Construct without making any API calls.
    client = FirefliesClient("dummy-key")
    status = StatusCache()
    return MeetingStore(client, status_cache=status)


def test_atomic_list_cache_write(empty_store: MeetingStore, cache_root: Path) -> None:
    """A successful save must produce a list.json file with no leftover .tmp."""
    empty_store._save_list_cache()  # noqa: SLF001
    assert (cache_root / "list.json").exists()
    leftover = list(cache_root.glob("list.json.*"))
    assert leftover == [], f"unexpected leftover tmp files: {leftover}"


def test_interrupted_list_cache_write_is_recoverable(
    empty_store: MeetingStore, cache_root: Path,
) -> None:
    """If a tmp file is left behind (simulated crash), the load path must ignore it
    and the previous list.json content must remain readable."""
    # Initial good save.
    empty_store._list_cache_time = 12345.0  # noqa: SLF001
    empty_store._save_list_cache()  # noqa: SLF001
    initial_content = (cache_root / "list.json").read_text()

    # Simulate a crash mid-write: drop a tmp file with garbage.
    (cache_root / "list.json.tmp").write_text("{ corrupted")

    # Loading must succeed and return the original content, not the garbage.
    empty_store._load_list_cache()  # noqa: SLF001
    assert (cache_root / "list.json").read_text() == initial_content


def test_detail_dir_without_sentinel_treated_as_uncached(
    empty_store: MeetingStore, cache_root: Path,
) -> None:
    """A half-populated detail directory (no .complete marker) must be re-fetched."""
    detail_dir = cache_root / "detail" / "MEET01"
    detail_dir.mkdir(parents=True)
    # Write only 2 of 5 files — the simulation of a crash mid-write.
    (detail_dir / "summary.md").write_bytes(b"partial")
    (detail_dir / "transcript.md").write_bytes(b"also partial")
    # NO .complete sentinel.

    # Inject a Meeting entry so get_uncached_meeting_ids has something to filter on.

    meeting = Meeting(
        id="MEET01",
        title="x",
        date_epoch_ms=1774891800000.0,
        is_live=False,
        meeting_info=MeetingInfo(summary_status="processed"),
    )
    empty_store._entries["MEET01"] = MeetingEntry(meeting=meeting, slug="x")  # noqa: SLF001

    pending = empty_store.get_uncached_meeting_ids()
    assert "MEET01" in pending, "half-populated dir without sentinel should still be pending"


def test_detail_dir_with_sentinel_treated_as_cached(
    empty_store: MeetingStore, cache_root: Path,
) -> None:
    """A complete detail directory (all 5 files plus .complete) must be skipped."""
    detail_dir = cache_root / "detail" / "MEET01"
    detail_dir.mkdir(parents=True)
    for filename in MEETING_FILES:
        (detail_dir / filename).write_bytes(b"x")
    (detail_dir / ".complete").touch()

    meeting = Meeting(
        id="MEET01",
        title="x",
        date_epoch_ms=1774891800000.0,
        is_live=False,
        meeting_info=MeetingInfo(summary_status="processed"),
    )
    empty_store._entries["MEET01"] = MeetingEntry(meeting=meeting, slug="x")  # noqa: SLF001

    pending = empty_store.get_uncached_meeting_ids()
    assert "MEET01" not in pending, "complete dir with sentinel should not be pending"


def test_save_detail_writes_sentinel_after_files(
    empty_store: MeetingStore, cache_root: Path,
) -> None:
    """_save_detail_to_disk must write all files AND the .complete marker."""
    files = {fn: b"content" for fn in MEETING_FILES}
    empty_store._save_detail_to_disk("MEET01", files)  # noqa: SLF001
    detail_dir = cache_root / "detail" / "MEET01"
    for fn in MEETING_FILES:
        assert (detail_dir / fn).read_bytes() == b"content"
    assert (detail_dir / ".complete").exists(), ".complete sentinel must be written"


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
        meeting_info=MeetingInfo(summary_status=summary_status),
        slug="simon-luke",
    )


def _entry(meeting: Meeting) -> MeetingEntry:
    return MeetingEntry(meeting=meeting, slug=meeting.slug)


def test_resolve_with_ghosts_folds_single_completed_zero_duration_collision(
    empty_store: MeetingStore,
) -> None:
    real = _entry(_meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0))
    ghost = _entry(_meeting("GHOST01", duration_mins=0.0))

    meetings, ghost_map, overlap_map = empty_store._resolve_with_ghosts_and_overlaps([ghost, real])  # noqa: SLF001

    assert list(meetings) == ["simon-luke"]
    assert meetings["simon-luke"].meeting.id == "REAL01"
    assert ghost_map == {"REAL01": "GHOST01"}
    assert overlap_map == {}


def test_list_meetings_folds_ghost_and_exposes_ghost_id(
    empty_store: MeetingStore,
) -> None:
    real = _entry(_meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0))
    ghost = _entry(_meeting("GHOST01", duration_mins=0.0))
    empty_store._entries = {"REAL01": real, "GHOST01": ghost}  # noqa: SLF001
    empty_store.mark_list_cache_fresh()

    meetings = empty_store.list_meetings(real.meeting.date_str)

    assert list(meetings) == ["simon-luke"]
    assert meetings["simon-luke"].meeting.id == "REAL01"
    assert empty_store.get_ghost_id("REAL01") == "GHOST01"


def test_ghost_files_are_read_from_disk_cache_only(
    empty_store: MeetingStore,
) -> None:
    real = _entry(_meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0))
    ghost = _entry(_meeting("GHOST01", duration_mins=0.0))
    empty_store._entries = {"REAL01": real, "GHOST01": ghost}  # noqa: SLF001
    empty_store.mark_list_cache_fresh()
    empty_store._save_detail_to_disk(  # noqa: SLF001
        "GHOST01",
        {filename: f"ghost {filename}".encode() for filename in MEETING_FILES},
    )
    empty_store.list_meetings(real.meeting.date_str)

    assert empty_store.get_ghost_file("REAL01", "meeting.json") == b"ghost meeting.json"
    assert empty_store.get_ghost_file_size("REAL01", "meeting.json") == len(b"ghost meeting.json")


def test_three_way_collision_does_not_fold_ghost(
    empty_store: MeetingStore,
) -> None:
    real = _entry(_meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0))
    ghost = _entry(_meeting("GHOST01", duration_mins=0.0))
    reconnect = _entry(_meeting("REAL02", duration_mins=5.0, epoch_offset_ms=2000.0))

    meetings, ghost_map, overlap_map = empty_store._resolve_with_ghosts_and_overlaps([  # noqa: SLF001
        ghost,
        real,
        reconnect,
    ])

    assert [entry.meeting.id for entry in meetings.values()] == ["GHOST01", "REAL01", "REAL02"]
    assert list(meetings) == ["simon-luke", "simon-luke-2", "simon-luke-3"]
    assert ghost_map == {}
    assert overlap_map == {}


def test_non_terminal_zero_duration_collision_does_not_fold(
    empty_store: MeetingStore,
) -> None:
    real = _entry(_meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0))
    candidate = _entry(_meeting("GHOST01", duration_mins=0.0, summary_status=""))

    meetings, ghost_map, overlap_map = empty_store._resolve_with_ghosts_and_overlaps([  # noqa: SLF001
        candidate,
        real,
    ])

    assert [entry.meeting.id for entry in meetings.values()] == ["GHOST01", "REAL01"]
    assert ghost_map == {}
    assert overlap_map == {}


def test_mine_listing_uses_full_date_ghost_context(
    empty_store: MeetingStore,
) -> None:
    empty_store.user_email = "alice@example.com"
    real = _entry(_meeting("REAL01", duration_mins=30.0, epoch_offset_ms=1000.0))
    ghost = _entry(_meeting("GHOST01", duration_mins=0.0, organizer_email="calendar@example.com"))
    empty_store._entries = {"REAL01": real, "GHOST01": ghost}  # noqa: SLF001
    empty_store.mark_list_cache_fresh()

    meetings = empty_store.list_meetings_mine(real.meeting.date_str)

    assert list(meetings) == ["simon-luke"]
    assert meetings["simon-luke"].meeting.id == "REAL01"
    assert empty_store.get_ghost_id("REAL01") == "GHOST01"


def test_split_overlaps_folds_primary_overlaps_and_leaves_separate_collision(
    empty_store: MeetingStore,
) -> None:
    primary = _entry(_meeting("PRIMARY01", duration_mins=60.0))
    overlap = _entry(_meeting("OVERLAP01", duration_mins=10.0, epoch_offset_ms=5 * 60_000.0))
    separate = _entry(_meeting("SEPARATE01", duration_mins=15.0, epoch_offset_ms=90 * 60_000.0))

    meetings, ghost_map, overlap_map = empty_store._resolve_with_ghosts_and_overlaps([  # noqa: SLF001
        separate,
        overlap,
        primary,
    ])

    assert [entry.meeting.id for entry in meetings.values()] == ["PRIMARY01", "SEPARATE01"]
    assert list(meetings) == ["simon-luke", "simon-luke-2"]
    assert ghost_map == {}
    assert overlap_map == {"PRIMARY01": ["OVERLAP01"]}


def test_overlap_accessors_return_ids_dirnames_and_cached_file(
    empty_store: MeetingStore,
) -> None:
    primary = _entry(_meeting("PRIMARY01", duration_mins=60.0))
    overlap_a = _entry(_meeting("OVERLAP01", duration_mins=10.0, epoch_offset_ms=10 * 60_000.0))
    overlap_b = _entry(_meeting("OVERLAP02", duration_mins=12.0, epoch_offset_ms=20 * 60_000.0))
    empty_store._entries = {  # noqa: SLF001
        entry.meeting.id: entry
        for entry in (primary, overlap_b, overlap_a)
    }
    empty_store.mark_list_cache_fresh()
    empty_store._save_detail_to_disk(  # noqa: SLF001
        "OVERLAP01",
        {filename: f"overlap {filename}".encode() for filename in MEETING_FILES},
    )

    meetings = empty_store.list_meetings(primary.meeting.date_str)

    assert list(meetings) == ["simon-luke"]
    assert empty_store.get_overlap_ids("PRIMARY01") == ["OVERLAP01", "OVERLAP02"]
    assert empty_store.get_overlap_dirnames("PRIMARY01") == ["overlap", "overlap-2"]
    assert empty_store.get_overlap_id_for_dirname("PRIMARY01", "overlap-2") == "OVERLAP02"
    assert empty_store.get_overlap_file("PRIMARY01", "overlap", "meeting.json") == b"overlap meeting.json"


def _cache_meeting_json(
    store: MeetingStore,
    meeting: Meeting,
    sentences: list[Sentence],
) -> None:
    detail = TranscriptDetail(meeting=meeting, sentences=sentences)
    files = {filename: b"" for filename in MEETING_FILES}
    files["meeting.json"] = render_meeting_json(meeting, detail).encode()
    store._save_detail_to_disk(meeting.id, files)  # noqa: SLF001


def test_overlap_warning_reports_sentences_missing_from_primary(
    empty_store: MeetingStore,
) -> None:
    primary = _entry(_meeting("PRIMARY01", duration_mins=60.0))
    overlap = _entry(_meeting("OVERLAP01", duration_mins=10.0, epoch_offset_ms=5 * 60_000.0))
    empty_store._entries = {  # noqa: SLF001
        "PRIMARY01": primary,
        "OVERLAP01": overlap,
    }
    empty_store.mark_list_cache_fresh()
    _cache_meeting_json(
        empty_store,
        primary.meeting,
        [
            Sentence(index=0, text="Shared sentence.", start_time=1.0, end_time=2.0, speaker_name="Alice"),
            Sentence(index=1, text="Primary only.", start_time=3.0, end_time=4.0, speaker_name="Alice"),
        ],
    )
    _cache_meeting_json(
        empty_store,
        overlap.meeting,
        [
            Sentence(index=0, text=" shared sentence. ", start_time=1.0, end_time=2.0, speaker_name="Alice"),
            Sentence(index=1, text="Overlap only detail.", start_time=3.0, end_time=4.0, speaker_name="Bob"),
        ],
    )
    empty_store.list_meetings(primary.meeting.date_str)

    warning = empty_store.get_overlap_warning("PRIMARY01").decode()

    assert "warning: overlap-not-superset" in warning
    assert "NOT a strict superset" in warning
    assert "## From `overlap/` (ID: OVERLAP01)" in warning
    assert "| 0:03 | Bob | Overlap only detail. |" in warning
    assert "shared sentence" not in warning.lower()


def test_overlap_warning_returns_clean_message_when_primary_is_superset(
    empty_store: MeetingStore,
) -> None:
    primary = _entry(_meeting("PRIMARY01", duration_mins=60.0))
    overlap = _entry(_meeting("OVERLAP01", duration_mins=10.0, epoch_offset_ms=5 * 60_000.0))
    empty_store._entries = {  # noqa: SLF001
        "PRIMARY01": primary,
        "OVERLAP01": overlap,
    }
    empty_store.mark_list_cache_fresh()
    _cache_meeting_json(
        empty_store,
        primary.meeting,
        [
            Sentence(index=0, text="Shared sentence.", start_time=1.0, end_time=2.0, speaker_name="Alice"),
            Sentence(index=1, text="Overlap only detail.", start_time=3.0, end_time=4.0, speaker_name="Bob"),
        ],
    )
    _cache_meeting_json(
        empty_store,
        overlap.meeting,
        [
            Sentence(index=0, text=" shared sentence. ", start_time=1.0, end_time=2.0, speaker_name="Alice"),
            Sentence(index=1, text="overlap only detail.", start_time=3.0, end_time=4.0, speaker_name="Bob"),
        ],
    )
    empty_store.list_meetings(primary.meeting.date_str)

    warning = empty_store.get_overlap_warning("PRIMARY01").decode()

    assert "warning: overlap-superset" in warning
    assert "No missing sentences were found" in warning
    assert "NOT a strict superset" not in warning
