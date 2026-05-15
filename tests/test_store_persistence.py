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

import json
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from fireflies_meetings.api import FirefliesClient, TranscriptNotFoundError
from fireflies_meetings.models import Meeting, MeetingInfo, Sentence, TranscriptDetail
from fireflies_meetings.renderer import render_meeting_json
from fireflies_meetings.status_cache import StatusCache
from fireflies_meetings.store import (
    MEETING_FILES,
    MeetingEntry,
    MeetingStore,
    _merge_refresh_entry,
)

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


def test_merge_refresh_entry_returns_new_when_no_existing() -> None:
    new = _entry(_meeting("MEET01", duration_mins=30.0))
    assert _merge_refresh_entry(None, new) is new


def test_merge_refresh_entry_preserves_existing_date_when_new_has_zero() -> None:
    """Status-API ghost without startTime must NOT overwrite a watch_meeting'd date."""
    existing_meeting = Meeting(
        id="MEET01",
        title="All-Hands",
        date_epoch_ms=1778196600000.0,  # 2026-05-08
        is_live=True,
        meeting_info=MeetingInfo(summary_status=""),
        slug="all-hands",
    )
    existing = MeetingEntry(meeting=existing_meeting, slug="all-hands")

    # New entry from a refresh source that lost the date (e.g. status-API ghost
    # leaking through; defense-in-depth).
    new_meeting = Meeting(
        id="MEET01",
        title="All-Hands",
        date_epoch_ms=0.0,
        is_live=False,
        meeting_info=MeetingInfo(summary_status=""),
        slug="all-hands",
    )
    new = MeetingEntry(meeting=new_meeting, slug="all-hands")

    merged = _merge_refresh_entry(existing, new)
    # Existing date preserved.
    assert merged.meeting.date_epoch_ms == 1778196600000.0
    assert merged.meeting.date_str == "2026-05-08"
    # is_live preserved (existing was live, new is not, summary not terminal).
    assert merged.meeting.is_live is True


def test_merge_refresh_entry_takes_new_date_when_existing_was_zero() -> None:
    """When existing has no date but new does, new wins (the normal upgrade path)."""
    existing_meeting = Meeting(
        id="MEET01",
        title="All-Hands",
        date_epoch_ms=0.0,
        is_live=True,
        meeting_info=MeetingInfo(summary_status=""),
        slug="all-hands",
    )
    existing = MeetingEntry(meeting=existing_meeting, slug="all-hands")

    new_meeting = Meeting(
        id="MEET01",
        title="All-Hands",
        date_epoch_ms=1778196600000.0,
        is_live=False,
        meeting_info=MeetingInfo(summary_status=""),
        slug="all-hands",
    )
    new = MeetingEntry(meeting=new_meeting, slug="all-hands")

    merged = _merge_refresh_entry(existing, new)
    assert merged.meeting.date_epoch_ms == 1778196600000.0
    # is_live still preserved from the existing live flag.
    assert merged.meeting.is_live is True


def _seed_disk_meeting(
    cache_root: Path,
    meeting_id: str,
    *,
    access_logs: list[dict[str, str]],
    summary_status: str = "processed",
) -> Path:
    """Write a minimal meeting.json + sentinel under the detail cache."""
    detail_dir = cache_root / "detail" / meeting_id
    detail_dir.mkdir(parents=True)
    for filename in MEETING_FILES:
        (detail_dir / filename).write_bytes(b"x")
    (detail_dir / "meeting.json").write_text(json.dumps({
        "id": meeting_id,
        "title": meeting_id,
        "date": "2026-04-15",
        "meeting_info": {"summary_status": summary_status},
        "access_logs": access_logs,
    }))
    (detail_dir / ".complete").touch()
    return detail_dir


def test_force_refresh_evicts_completed_meetings_with_empty_access_logs(
    empty_store: MeetingStore, cache_root: Path,
) -> None:
    """SIGUSR1 must trigger a refetch for completed meetings whose access_logs
    are empty (the symptom of caching before session auth was configured)."""
    detail_dir = _seed_disk_meeting(cache_root, "MEET_EMPTY", access_logs=[])
    empty_store._status_cache.mark_completed("MEET_EMPTY")  # noqa: SLF001

    empty_store.force_refresh()

    # Sentinel must be gone so the next read forces a refetch.
    assert not (detail_dir / ".complete").exists()
    # Bodies stay in place as fallback if the refetch fails.
    assert (detail_dir / "meeting.json").exists()


def test_force_refresh_preserves_completed_meetings_with_populated_access_logs(
    empty_store: MeetingStore, cache_root: Path,
) -> None:
    """Meetings with non-empty access_logs must NOT be re-fetched on SIGUSR1
    — that would be a needless API storm across the whole cache."""
    detail_dir = _seed_disk_meeting(
        cache_root,
        "MEET_LOGS",
        access_logs=[{"timestamp": "2026-05-01T10:00:00Z", "user_name": "alice",
                      "user_email": "a@b", "action": "viewed"}],
    )
    empty_store._status_cache.mark_completed("MEET_LOGS")  # noqa: SLF001

    empty_store.force_refresh()

    assert (detail_dir / ".complete").exists()


def test_force_refresh_skips_missing_from_api_stubs(
    empty_store: MeetingStore, cache_root: Path,
) -> None:
    """Stubs for transcripts deleted on Fireflies' side can never gain access
    logs — refetching them just produces another stub."""
    detail_dir = _seed_disk_meeting(
        cache_root, "MEET_GHOST",
        access_logs=[],
        summary_status="missing_from_api",
    )
    empty_store._status_cache.mark_completed("MEET_GHOST")  # noqa: SLF001

    empty_store.force_refresh()

    assert (detail_dir / ".complete").exists()


class _NotFoundClient:
    """Fake client whose get_transcript always 404s."""

    def get_transcript(self, meeting_id: str) -> TranscriptDetail:
        raise TranscriptNotFoundError(f"Transcript {meeting_id} no longer exists")


def test_fetch_detail_preserves_real_cache_when_api_404s(
    cache_root: Path,
) -> None:
    """If the API forgets a meeting we have cached content for, we must NOT
    overwrite the cache with a stub — Fireflies sometimes drops transcripts
    and the cache may be the only remaining copy."""
    detail_dir = _seed_disk_meeting(
        cache_root, "MEET_KEEP",
        access_logs=[],
        summary_status="processed",
    )
    real_summary = b"# real meeting summary\nsome content"
    (detail_dir / "summary.md").write_bytes(real_summary)
    # No sentinel (mimics post-force-refresh state).
    (detail_dir / ".complete").unlink()

    status = StatusCache()
    status.mark_completed("MEET_KEEP")
    store = MeetingStore(
        cast(FirefliesClient, _NotFoundClient()), status_cache=status,
    )

    meeting = Meeting(
        id="MEET_KEEP",
        title="MEET_KEEP",
        date_epoch_ms=1774891800000.0,
        meeting_info=MeetingInfo(summary_status="processed"),
        slug="meet-keep",
    )

    cached, _changed = store._fetch_detail(meeting)  # noqa: SLF001

    assert cached is not None, "should return preserved cache, not None"
    # Cache files unchanged on disk.
    assert (detail_dir / "summary.md").read_bytes() == real_summary
    # Sentinel restored.
    assert (detail_dir / ".complete").exists()
    # meeting.json must still report the real status, not the stub.
    raw = json.loads((detail_dir / "meeting.json").read_text())
    assert raw["meeting_info"]["summary_status"] == "processed"


def test_fetch_detail_writes_stub_when_no_real_cache_on_404(
    cache_root: Path,
) -> None:
    """First-fetch 404 must still produce the stub (existing behavior).

    Without prior cached content there's nothing to preserve."""
    status = StatusCache()
    store = MeetingStore(
        cast(FirefliesClient, _NotFoundClient()), status_cache=status,
    )

    meeting = Meeting(
        id="MEET_NEW",
        title="MEET_NEW",
        date_epoch_ms=1774891800000.0,
        meeting_info=MeetingInfo(summary_status="processed"),
        slug="meet-new",
    )

    cached, _changed = store._fetch_detail(meeting)  # noqa: SLF001

    assert cached is not None
    # Stub written.
    detail_dir = cache_root / "detail" / "MEET_NEW"
    assert (detail_dir / ".complete").exists()
    raw = json.loads((detail_dir / "meeting.json").read_text())
    assert raw["meeting_info"]["summary_status"] == "missing_from_api"


def test_backfill_one_preserves_real_cache_when_api_404s(
    cache_root: Path,
) -> None:
    """Same guarantee as _fetch_detail but for the background backfill path."""
    detail_dir = _seed_disk_meeting(
        cache_root, "MEET_BACKFILL",
        access_logs=[],
        summary_status="processed",
    )
    real_transcript = b"# real transcript\n* alice (00:00): hello"
    (detail_dir / "transcript.md").write_bytes(real_transcript)
    # No sentinel: backfill_one will treat it as uncached and re-fetch.
    (detail_dir / ".complete").unlink()

    status = StatusCache()
    store = MeetingStore(
        cast(FirefliesClient, _NotFoundClient()), status_cache=status,
    )
    meeting = Meeting(
        id="MEET_BACKFILL",
        title="MEET_BACKFILL",
        date_epoch_ms=1774891800000.0,
        meeting_info=MeetingInfo(summary_status="processed"),
        slug="meet-backfill",
    )
    store._entries["MEET_BACKFILL"] = MeetingEntry(  # noqa: SLF001
        meeting=meeting, slug=meeting.slug,
    )

    store.backfill_one("MEET_BACKFILL")

    assert (detail_dir / "transcript.md").read_bytes() == real_transcript
    assert (detail_dir / ".complete").exists()
    raw = json.loads((detail_dir / "meeting.json").read_text())
    assert raw["meeting_info"]["summary_status"] == "processed"
