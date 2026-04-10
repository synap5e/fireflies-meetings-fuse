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
from fireflies_meetings.models import Meeting, MeetingInfo
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
