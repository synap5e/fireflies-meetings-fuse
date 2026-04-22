"""Tests for the renderer module."""

from __future__ import annotations

import json

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
from fireflies_meetings.renderer import (
    render_meeting_json,
    render_open_script,
    render_participants,
    render_summary,
    render_transcript,
    render_views,
)


def _make_meeting(**kwargs: object) -> Meeting:
    defaults: dict[str, object] = {
        "id": "MEET01",
        "title": "Team Standup",
        "date_str": "2026-03-31",
        "date_epoch_ms": 1774891800000.0,
        "duration_mins": 15.0,
        "is_live": False,
        "organizer_email": "alice@example.com",
        "participants": ["alice@example.com", "bob@example.com"],
        "transcript_url": "https://app.fireflies.ai/view/MEET01",
        "meeting_info": MeetingInfo(fred_joined=True, silent_meeting=False, summary_status="processed"),
    }
    defaults.update(kwargs)
    return Meeting(**defaults)  # type: ignore[arg-type]


def _make_detail(meeting: Meeting, **kwargs: object) -> TranscriptDetail:
    sentences = [
        Sentence(index=0, text="Hello everyone.", start_time=5.0, end_time=6.5, speaker_name="Alice"),
        Sentence(index=1, text="Good morning.", start_time=7.0, end_time=8.0, speaker_name="Bob"),
        Sentence(index=2, text="Let's get started.", start_time=8.5, end_time=10.0, speaker_name="Alice"),
    ]
    summary = Summary(
        keywords="standup, updates",
        action_items="- Alice: write tests\n- Bob: review PR",
        overview="Daily standup covering updates.",
        gist="Quick standup.",
        short_summary="The team synced on daily updates.",
    )
    return TranscriptDetail(
        meeting=meeting,
        sentences=kwargs.get("sentences", sentences),  # type: ignore[arg-type]
        speakers=[Speaker(id=0, name="Alice"), Speaker(id=1, name="Bob")],
        summary=kwargs.get("summary", summary),  # type: ignore[arg-type]
        attendees=[
            MeetingAttendee(display_name="Alice Smith", email="alice@example.com"),
            MeetingAttendee(display_name=None, email="bob@example.com"),
        ],
    )


def test_render_summary_completed() -> None:
    meeting = _make_meeting()
    detail = _make_detail(meeting)
    result = render_summary(meeting, detail)

    assert 'title: "Team Standup"' in result
    assert "date: 2026-03-31" in result
    assert 'organizer: "alice@example.com"' in result
    assert 'status: "processed"' in result
    assert "## Summary" in result
    assert "The team synced on daily updates." in result
    assert "## Action Items" in result
    assert "Alice: write tests" in result
    assert "## Keywords" in result
    assert "standup" in result


def test_render_summary_no_summary() -> None:
    meeting = _make_meeting(is_live=True)
    detail = _make_detail(meeting, summary=None)
    result = render_summary(meeting, detail)
    assert "in progress" in result.lower()


def test_render_summary_missing_from_api() -> None:
    meeting = _make_meeting(
        meeting_info=MeetingInfo(summary_status="missing_from_api"),
    )
    detail = _make_detail(meeting, summary=None)
    result = render_summary(meeting, detail)
    assert "no longer available" in result.lower()
    assert 'status: "missing_from_api"' in result


def test_render_transcript_timestamps() -> None:
    meeting = _make_meeting()
    detail = _make_detail(meeting)
    result = render_transcript(meeting, detail)

    assert "### Alice" in result
    assert "### Bob" in result
    assert "[00:05] Hello everyone." in result
    assert "[00:07] Good morning." in result
    assert "[00:08] Let's get started." in result


def test_render_transcript_partial_error() -> None:
    meeting = _make_meeting(is_live=True)
    detail = _make_detail(
        meeting,
        sentences=[],
    ).model_copy(update={
        "speakers": [],
        "transcript_error": "Fireflies returned INTERNAL_SERVER_ERROR for transcript.sentences",
    })
    result = render_transcript(meeting, detail)

    assert "temporarily unavailable" in result
    assert "INTERNAL_SERVER_ERROR" in result
    assert "No transcript available" not in result


def test_render_transcript_groups_same_speaker() -> None:
    meeting = _make_meeting()
    sentences = [
        Sentence(index=0, text="First.", start_time=5.0, end_time=6.0, speaker_name="Alice"),
        Sentence(index=1, text="Second.", start_time=7.0, end_time=8.0, speaker_name="Alice"),
        Sentence(index=2, text="Third.", start_time=9.0, end_time=10.0, speaker_name="Bob"),
    ]
    detail = _make_detail(meeting, sentences=sentences)
    result = render_transcript(meeting, detail)
    # Alice should only appear once as a header (consecutive turns)
    assert result.count("### Alice") == 1
    assert result.count("### Bob") == 1


def test_render_participants() -> None:
    meeting = _make_meeting()
    detail = _make_detail(meeting)
    result = render_participants(meeting, detail)

    assert "| Participant | Talk time | % |" in result
    assert "Alice" in result
    assert "Bob" in result


def test_render_participants_no_sentences() -> None:
    meeting = _make_meeting()
    detail = _make_detail(meeting, sentences=[])
    result = render_participants(meeting, detail)
    assert "No participant data" in result


def test_render_meeting_json() -> None:
    meeting = _make_meeting()
    detail = _make_detail(meeting)
    result = render_meeting_json(meeting, detail)
    data = json.loads(result)

    assert data["id"] == "MEET01"
    assert data["title"] == "Team Standup"
    assert len(data["transcript"]) == 3
    assert data["transcript"][0]["speaker_name"] == "Alice"
    assert data["summary"]["gist"] == "Quick standup."


def test_render_open_script() -> None:
    meeting = _make_meeting()
    result = render_open_script(meeting)
    assert "xdg-open" in result
    assert "MEET01" in result


# === YAML frontmatter escaping ===
#
# Frontmatter is rendered as JSON-encoded double-quoted strings
# (JSON ⊂ YAML for double-quoted scalars), so any control char, quote,
# or backslash gets escaped properly. These tests lock in that no field
# can be used to inject extra YAML lines or break the parser.


def _frontmatter_lines(rendered: str) -> list[str]:
    """Extract the lines between the first pair of `---` markers."""
    lines = rendered.split("\n")
    start = lines.index("---")
    end = lines.index("---", start + 1)
    return lines[start + 1 : end]


def test_frontmatter_escapes_title_newline() -> None:
    meeting = _make_meeting(title="line one\nline two\nINJECT: bad")
    detail = _make_detail(meeting)
    result = render_summary(meeting, detail)
    fm = _frontmatter_lines(result)
    # Title line must NOT have been split into multiple physical lines.
    title_lines = [line for line in fm if line.startswith("title:")]
    assert len(title_lines) == 1
    # The injected "INJECT: bad" line must NOT appear as a frontmatter key.
    assert not any(line.startswith("INJECT:") for line in fm)
    # The escaped form should be present.
    assert "\\n" in title_lines[0]


def test_frontmatter_escapes_title_double_quote() -> None:
    meeting = _make_meeting(title='evil "quote" here')
    detail = _make_detail(meeting)
    result = render_summary(meeting, detail)
    fm = _frontmatter_lines(result)
    title_lines = [line for line in fm if line.startswith("title:")]
    assert len(title_lines) == 1
    # Quotes inside the title must be escaped, not raw.
    assert '\\"' in title_lines[0]


def test_frontmatter_escapes_title_backslash() -> None:
    meeting = _make_meeting(title="path\\to\\thing")
    detail = _make_detail(meeting)
    result = render_summary(meeting, detail)
    fm = _frontmatter_lines(result)
    title_lines = [line for line in fm if line.startswith("title:")]
    assert len(title_lines) == 1
    # Backslash must be doubled.
    assert "\\\\" in title_lines[0]


def test_frontmatter_escapes_organizer() -> None:
    """Organizer is also a string field — must be escaped, not raw."""
    meeting = _make_meeting(organizer_email='alice@example.com"\ninjected: bad')
    detail = _make_detail(meeting)
    result = render_summary(meeting, detail)
    fm = _frontmatter_lines(result)
    assert not any(line.startswith("injected:") for line in fm)


def test_frontmatter_escapes_url() -> None:
    meeting = _make_meeting(transcript_url='https://x"\ninjected: bad')
    detail = _make_detail(meeting)
    result = render_summary(meeting, detail)
    fm = _frontmatter_lines(result)
    assert not any(line.startswith("injected:") for line in fm)


def test_frontmatter_unicode_preserved() -> None:
    """Unicode in title should round-trip cleanly through the escape."""
    meeting = _make_meeting(title="standup — w/ café crew 🎉")
    detail = _make_detail(meeting)
    result = render_summary(meeting, detail)
    # The original content is preserved (just wrapped in quotes).
    assert "café" in result
    assert "🎉" in result


def test_transcript_frontmatter_escapes_title() -> None:
    """transcript.md and participants.md share the same risk surface."""
    meeting = _make_meeting(title='evil"\ninjected: bad')
    detail = _make_detail(meeting)
    transcript = render_transcript(meeting, detail)
    participants = render_participants(meeting, detail)
    for rendered in (transcript, participants):
        fm = _frontmatter_lines(rendered)
        assert not any(line.startswith("injected:") for line in fm)


def test_render_views_empty_non_live() -> None:
    """No access logs on a completed meeting -> empty-state body, still valid markdown."""
    meeting = _make_meeting()
    detail = _make_detail(meeting)
    result = render_views(meeting, detail)
    assert "entries: 0" in result
    assert "session auth is not configured" in result


def test_render_views_empty_live() -> None:
    """Empty-state message is different when the meeting is still live."""
    meeting = _make_meeting(is_live=True, meeting_info=MeetingInfo(summary_status=""))
    detail = _make_detail(meeting)
    result = render_views(meeting, detail)
    assert "not collected while the meeting is live" in result


def test_render_views_populated_sorts_newest_first() -> None:
    meeting = _make_meeting()
    detail = _make_detail(meeting).model_copy(update={
        "access_logs": [
            AccessLogEntry(
                id="a", user_id="u1", user_email="alice@example.com",
                user_name="Alice", action="view_summary",
                timestamp="2026-04-01T10:00:00.000Z",
            ),
            AccessLogEntry(
                id="b", user_id="u2", user_email="bob@example.com",
                user_name="Bob", action="view_summary",
                timestamp="2026-04-03T10:00:00.000Z",
            ),
        ],
    })
    result = render_views(meeting, detail)
    assert "entries: 2" in result
    assert "| When | Who | Email | Action |" in result
    bob_idx = result.index("Bob")
    alice_idx = result.index("Alice")
    assert bob_idx < alice_idx, "newest (2026-04-03) should render before older (2026-04-01)"


def test_render_views_frontmatter_escapes_title() -> None:
    meeting = _make_meeting(title='evil"\ninjected: bad')
    detail = _make_detail(meeting)
    result = render_views(meeting, detail)
    fm = _frontmatter_lines(result)
    assert not any(line.startswith("injected:") for line in fm)


def test_meeting_json_includes_access_logs() -> None:
    meeting = _make_meeting()
    detail = _make_detail(meeting).model_copy(update={
        "access_logs": [
            AccessLogEntry(
                id="a", user_id="u1", user_email="alice@example.com",
                user_name="Alice", action="view_summary",
                timestamp="2026-04-01T10:00:00.000Z",
            ),
        ],
    })
    result = render_meeting_json(meeting, detail)
    data = json.loads(result)
    assert data["access_logs"] == [{
        "id": "a", "user_id": "u1", "user_email": "alice@example.com",
        "user_name": "Alice", "action": "view_summary",
        "timestamp": "2026-04-01T10:00:00.000Z",
    }]
