"""Render meeting data to markdown, JSON, and shell scripts."""

from __future__ import annotations

import json
import shlex
from collections import defaultdict

from .models import Meeting, TranscriptDetail


def render_summary(meeting: Meeting, detail: TranscriptDetail) -> str:
    """Render summary.md with frontmatter, summary text, action items, and keywords."""
    parts: list[str] = []

    parts.append("---")
    parts.append(f"title: {_yaml_str(meeting.title)}")
    parts.append(f"date: {meeting.date_str}")
    parts.append(f"organizer: {_yaml_str(meeting.organizer_email)}")
    if meeting.duration_mins:
        mins = int(meeting.duration_mins)
        parts.append(f"duration: {mins}m")
    parts.append(f"participants: {len(meeting.participants)}")
    if meeting.transcript_url:
        parts.append(f"url: {_yaml_str(meeting.transcript_url)}")
    status = "live" if meeting.is_live else meeting.meeting_info.summary_status or "completed"
    parts.append(f"status: {_yaml_str(status)}")
    parts.append("---")
    parts.append("")

    summary = detail.summary
    if summary is None:
        if meeting.is_live:
            parts.append("*Meeting is in progress. Summary will be available after it ends.*")
        elif meeting.meeting_info.summary_status == "skipped":
            parts.append("*Summary skipped by Fireflies (likely too short or filtered).*")
        else:
            parts.append("*Summary not yet available.*")
        parts.append("")
        return "\n".join(parts)

    body = summary.short_summary or summary.overview or summary.gist
    if body:
        parts.append("## Summary")
        parts.append("")
        parts.append(body)
        parts.append("")

    if summary.action_items:
        parts.append("## Action Items")
        parts.append("")
        parts.append(summary.action_items)
        parts.append("")

    if summary.keywords:
        parts.append("## Keywords")
        parts.append("")
        parts.append(summary.keywords)
        parts.append("")

    return "\n".join(parts)


def render_transcript(meeting: Meeting, detail: TranscriptDetail) -> str:
    """Render transcript.md: speaker-attributed sentences with [MM:SS] timestamps."""
    parts: list[str] = []

    parts.append("---")
    parts.append(f"title: {_yaml_str(meeting.title)}")
    parts.append(f"date: {meeting.date_str}")
    parts.append(f"speakers: {len(detail.speakers)}")
    if meeting.is_live:
        parts.append("status: live")
    parts.append("---")
    parts.append("")

    if not detail.sentences:
        parts.append("*No transcript available.*")
        parts.append("")
        return "\n".join(parts)

    prev_speaker = ""
    for sentence in detail.sentences:
        offset_sec = max(0, int(sentence.start_time))
        minutes = offset_sec // 60
        seconds = offset_sec % 60
        timestamp = f"[{minutes:02d}:{seconds:02d}]"

        if sentence.speaker_name != prev_speaker:
            parts.append(f"### {sentence.speaker_name}")
            parts.append("")
            prev_speaker = sentence.speaker_name

        parts.append(f"{timestamp} {sentence.text}")
        parts.append("")

    return "\n".join(parts)


def render_participants(meeting: Meeting, detail: TranscriptDetail) -> str:
    """Render participants.md: speaker list with computed talk time from sentence timestamps."""
    parts: list[str] = []

    parts.append("---")
    parts.append(f"title: {_yaml_str(meeting.title)}")
    parts.append(f"date: {meeting.date_str}")
    parts.append(f"participants: {len(meeting.participants)}")
    parts.append("---")
    parts.append("")

    speaker_secs: dict[str, float] = defaultdict(float)
    for sentence in detail.sentences:
        speaker_secs[sentence.speaker_name] += max(0.0, sentence.end_time - sentence.start_time)

    if not speaker_secs:
        parts.append("*No participant data available.*")
        parts.append("")
        return "\n".join(parts)

    total_secs = sum(speaker_secs.values())
    sorted_speakers = sorted(speaker_secs.items(), key=lambda x: x[1], reverse=True)

    parts.append("| Participant | Talk time | % |")
    parts.append("|-------------|-----------|---|")
    for name, secs in sorted_speakers:
        mins = int(secs) // 60
        secs_rem = int(secs) % 60
        pct = (secs / total_secs * 100) if total_secs > 0 else 0
        parts.append(f"| {name} | {mins}m {secs_rem:02d}s | {pct:.0f}% |")
    parts.append("")

    return "\n".join(parts)


def render_meeting_json(meeting: Meeting, detail: TranscriptDetail) -> str:
    """Render meeting.json with all meeting data."""
    data = {
        "id": meeting.id,
        "title": meeting.title,
        "date": meeting.date_str,
        "date_epoch_ms": meeting.date_epoch_ms,
        "duration_mins": meeting.duration_mins,
        "is_live": meeting.is_live,
        "organizer_email": meeting.organizer_email,
        "participants": meeting.participants,
        "transcript_url": meeting.transcript_url,
        "meeting_info": {
            "fred_joined": meeting.meeting_info.fred_joined,
            "silent_meeting": meeting.meeting_info.silent_meeting,
            "summary_status": meeting.meeting_info.summary_status,
        },
        "speakers": [{"id": s.id, "name": s.name} for s in detail.speakers],
        "attendees": [
            {"display_name": a.display_name, "email": a.email} for a in detail.attendees
        ],
        "summary": {
            "keywords": detail.summary.keywords,
            "action_items": detail.summary.action_items,
            "overview": detail.summary.overview,
            "gist": detail.summary.gist,
            "short_summary": detail.summary.short_summary,
        } if detail.summary else None,
        "transcript": [
            {
                "index": s.index,
                "speaker_name": s.speaker_name,
                "text": s.text,
                "start_time": s.start_time,
                "end_time": s.end_time,
            }
            for s in detail.sentences
        ],
    }
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def render_open_script(meeting: Meeting) -> str:
    """Render open.sh that opens the meeting transcript in the browser."""
    url = shlex.quote(meeting.transcript_url)
    return f"#!/usr/bin/env bash\nexec xdg-open {url}\n"


def _yaml_str(text: str) -> str:
    """Encode an arbitrary string as a YAML scalar.

    JSON's double-quoted string syntax is a subset of YAML's, and
    `json.dumps` correctly handles control chars, embedded quotes,
    backslashes, and unicode — including the things `_escape_yaml`
    used to miss (newlines, tabs, etc) which let a malicious title
    inject extra YAML lines into the frontmatter.
    """
    return json.dumps(text, ensure_ascii=False)
