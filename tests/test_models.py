"""Tests for the meeting_id allowlist validator on the Meeting model.

The Fireflies API controls meeting IDs, but the rest of the codebase uses
them directly in filesystem path construction (status_cache, detail dir,
list cache lookups). The validator at the boundary ensures that any
malformed or hostile ID is rejected at parse time, so no downstream code
ever sees an unsafe value.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fireflies_meetings.models import Meeting, MeetingInfo


def _make(id_value: str) -> dict[str, object]:
    return {
        "id": id_value,
        "title": "x",
        "date": 1774891800000.0,
    }


@pytest.mark.parametrize(
    "valid_id",
    [
        "MEET01",
        "abc123",
        "ABCDEF0123456789",
        "with-dashes",
        "with_underscores",
        "Mixed-Case_01",
        "a",  # single char
        "A" * 64,  # max length
    ],
)
def test_valid_meeting_ids_accepted(valid_id: str) -> None:
    meeting = Meeting.model_validate(_make(valid_id))
    assert meeting.id == valid_id


@pytest.mark.parametrize(
    "bad_id",
    [
        "",  # empty
        "A" * 65,  # too long
        "../etc/passwd",  # path traversal
        "/absolute",
        "foo/bar",  # slash inside
        "foo\\bar",  # backslash
        "foo bar",  # space
        "foo.bar",  # dot
        "foo\x00bar",  # null byte
        "foo\nbar",  # newline
        "foo\rbar",  # carriage return
        "..",
        ".",
        "foo$bar",
        "foo;bar",
        "foo&bar",
        "foo|bar",
        "\u202eevil",  # right-to-left override
    ],
)
def test_invalid_meeting_ids_rejected(bad_id: str) -> None:
    with pytest.raises(ValidationError):
        Meeting.model_validate(_make(bad_id))


def test_summary_status_not_found_normalizes_to_missing_from_api() -> None:
    meeting = Meeting(
        id="MEET01",
        title="x",
        date_epoch_ms=1774891800000.0,
        meeting_info=MeetingInfo(summary_status="not_found"),
    )
    assert meeting.meeting_info.summary_status == "missing_from_api"
    assert meeting.is_completed
