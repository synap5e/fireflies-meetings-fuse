#!/usr/bin/env python3
"""Compare resolver-driven projection with the pre-resolver projection rules."""

from __future__ import annotations

import json
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from fireflies_meetings.access_logs import ACCESS_LOGS_PENDING, AccessLogsOk
from fireflies_meetings.capture import CaptureSnapshot, CaptureStore, default_cache_dir
from fireflies_meetings.models import Meeting, TranscriptDetail
from fireflies_meetings.projection import (
    ACCESS_LOG_PENDING,
    SUMMARY_PENDING,
    TRANSCRIPT_PENDING,
    CaptureState,
    ProjectedMeeting,
    ProjectionBuildOptions,
    build_projection_from_captures,
)
from fireflies_meetings.renderer import (
    render_meeting_json,
    render_open_script,
    render_participants,
    render_summary,
    render_transcript,
    render_views,
)
from fireflies_meetings.resolver import MeetingEvidence, ResolvedMeeting, resolve_meeting
from fireflies_meetings.slug import slugify

_FIELD_NAMES = (
    "title",
    "date_epoch_ms",
    "date_str",
    "duration_mins",
    "is_live",
    "organizer_email",
    "participants",
    "transcript_url",
    "slug",
    "meeting_info.summary_status",
    "meeting_info.fred_joined",
)


def _with_slug(meeting: Meeting) -> Meeting:
    if meeting.slug:
        return meeting
    slug = slugify(meeting.title) if meeting.title else meeting.id[:12]
    return meeting.model_copy(update={"slug": slug})


def _field(meeting: Meeting, name: str) -> object:
    if name == "meeting_info.summary_status":
        return meeting.meeting_info.summary_status
    if name == "meeting_info.fred_joined":
        return meeting.meeting_info.fred_joined
    return getattr(meeting, name)


def _legacy_meeting_map(snapshot: CaptureSnapshot) -> dict[str, Meeting]:
    meetings = {meeting.id: _with_slug(meeting) for meeting in snapshot.meetings}
    for meeting_id, detail in snapshot.details.items():
        meetings.setdefault(meeting_id, _with_slug(detail.meeting))
    return meetings


def _legacy_detail(
    meeting: Meeting,
    captured: TranscriptDetail | None,
    outcome: object,
) -> TranscriptDetail:
    logs = list(outcome.logs) if isinstance(outcome, AccessLogsOk) else []
    if captured is None:
        return TranscriptDetail(meeting=meeting, access_logs=logs)
    return captured.model_copy(
        update={
            "meeting": captured.meeting.model_copy(
                update={
                    "slug": meeting.slug,
                    "date_str": meeting.date_str,
                    "date_epoch_ms": meeting.date_epoch_ms,
                    "is_live": meeting.is_live or captured.meeting.is_live,
                }
            ),
            "access_logs": logs,
        }
    )


def _legacy_state(
    meeting: Meeting,
    captured: TranscriptDetail | None,
    *,
    has_access_log_capture: bool,
) -> CaptureState:
    detail_terminal = captured is not None and captured.meeting.summary_is_terminal
    if meeting.is_live and not meeting.summary_is_terminal and not detail_terminal:
        return "live"
    if captured is None or captured.transcript_error:
        return "partial"
    status = captured.meeting.meeting_info.summary_status or meeting.meeting_info.summary_status
    if status == "missing_from_api":
        return "captured"
    if captured.meeting.summary_is_terminal and has_access_log_capture:
        return "captured"
    return "partial"


def _legacy_render_files(
    meeting: Meeting,
    detail: TranscriptDetail,
    state: CaptureState,
    *,
    has_access_logs: bool,
) -> dict[str, bytes]:
    if state == "captured":
        summary = render_summary(detail.meeting, detail).encode()
        transcript = render_transcript(detail.meeting, detail).encode()
    else:
        summary = SUMMARY_PENDING
        transcript = TRANSCRIPT_PENDING if not detail.sentences else render_transcript(detail.meeting, detail).encode()
    views = render_views(detail.meeting, detail).encode() if has_access_logs else ACCESS_LOG_PENDING
    raw: object = json.loads(render_meeting_json(detail.meeting, detail))
    assert isinstance(raw, dict)
    meeting_json = cast("dict[str, object]", raw)
    meeting_json["capture_state"] = state
    return {
        "summary.md": summary,
        "transcript.md": transcript,
        "participants.md": render_participants(detail.meeting, detail).encode(),
        "meeting.json": (json.dumps(meeting_json, indent=2, ensure_ascii=False) + "\n").encode(),
        "open.sh": render_open_script(meeting).encode(),
        "views.md": views,
    }


def _resolve_all(snapshot: CaptureSnapshot, *, now_ms: float) -> dict[str, ResolvedMeeting]:
    listed = {meeting.id: meeting for meeting in snapshot.meetings}
    meeting_ids = dict.fromkeys((*listed, *snapshot.details))
    return {
        meeting_id: resolve_meeting(
            MeetingEvidence(
                list_meeting=listed.get(meeting_id),
                detail=snapshot.details.get(meeting_id),
                access_logs=snapshot.access_logs.get(meeting_id, ACCESS_LOGS_PENDING),
                live_captions={},
                terminal_seen=(
                    meeting_id in snapshot.details and snapshot.details[meeting_id].meeting.summary_is_terminal
                ),
                now_ms=now_ms,
            )
        )
        for meeting_id in meeting_ids
    }


def _conflict_causes(listed: Meeting | None, detail: TranscriptDetail | None) -> set[str]:
    if listed is None or detail is None:
        return set()
    detailed = detail.meeting
    causes: set[str] = set()
    for field_name, category in (
        ("title", "title"),
        ("date_epoch_ms", "date_epoch_ms"),
        ("duration_mins", "duration_mins"),
        ("is_live", "is_live"),
        ("participants", "participants"),
        ("meeting_info.summary_status", "summary_status"),
        ("meeting_info.fred_joined", "fred_joined"),
    ):
        if _field(listed, field_name) != _field(detailed, field_name):
            causes.add(category)
    if listed.date_str != detailed.date_str:
        causes.add("date_str")
    return causes


@dataclass
class _Comparison:
    cause_population: Counter[str] = field(default_factory=Counter)
    field_deltas: Counter[str] = field(default_factory=Counter)
    file_deltas: Counter[str] = field(default_factory=Counter)
    state_deltas: Counter[str] = field(default_factory=Counter)
    unexpected: dict[str, list[str]] = field(default_factory=dict)


@dataclass(frozen=True)
class _ComparisonContext:
    snapshot: CaptureSnapshot
    listed: dict[str, Meeting]
    comparison: _Comparison


def _compare_one(
    meeting_id: str,
    old_meeting: Meeting,
    projection_meeting: ProjectedMeeting,
    context: _ComparisonContext,
) -> None:
    snapshot = context.snapshot
    comparison = context.comparison
    captured = snapshot.details.get(meeting_id)
    outcome = snapshot.access_logs.get(meeting_id, ACCESS_LOGS_PENDING)
    old_detail = _legacy_detail(old_meeting, captured, outcome)
    old_state = _legacy_state(
        old_meeting,
        captured,
        has_access_log_capture=meeting_id in snapshot.access_logs,
    )
    old_files = _legacy_render_files(
        old_meeting,
        old_detail,
        old_state,
        has_access_logs=meeting_id in snapshot.access_logs,
    )
    causes = _conflict_causes(context.listed.get(meeting_id), captured)
    comparison.cause_population.update(causes)

    meeting_unexpected: list[str] = []
    for field_name in _FIELD_NAMES:
        if _field(old_meeting, field_name) == _field(projection_meeting.meeting, field_name):
            continue
        comparison.field_deltas[field_name] += 1
        expected_cause = field_name.removeprefix("meeting_info.")
        if expected_cause not in causes and not (field_name in {"date_epoch_ms", "date_str"} and "date_str" in causes):
            meeting_unexpected.append(f"field:{field_name}")

    if old_state != projection_meeting.capture_state:
        transition = f"{old_state}->{projection_meeting.capture_state}"
        comparison.state_deltas[transition] += 1
        if not causes:
            meeting_unexpected.append(f"state:{transition}")

    for file_name, old_content in old_files.items():
        if old_content == projection_meeting.files[file_name]:
            continue
        comparison.file_deltas[file_name] += 1
        if not causes:
            meeting_unexpected.append(f"file:{file_name}")
    if meeting_unexpected:
        comparison.unexpected[meeting_id] = meeting_unexpected


def main() -> None:
    cache = CaptureStore(default_cache_dir())
    snapshot = cache.read_snapshot()
    now_ms = time.time() * 1000
    first = _resolve_all(snapshot, now_ms=now_ms)
    second = _resolve_all(snapshot, now_ms=now_ms)
    determinism_delta = {meeting_id for meeting_id, resolved in first.items() if resolved != second[meeting_id]}

    projection = build_projection_from_captures(
        snapshot,
        ProjectionBuildOptions(now_ms=now_ms),
    )
    listed = {meeting.id: meeting for meeting in snapshot.meetings}
    legacy_meetings = _legacy_meeting_map(snapshot)
    comparison = _Comparison()
    context = _ComparisonContext(snapshot=snapshot, listed=listed, comparison=comparison)

    for meeting_id, old_meeting in legacy_meetings.items():
        _compare_one(
            meeting_id,
            old_meeting,
            projection.meetings[meeting_id],
            context,
        )

    lines = [
        "# Resolver shadow-comparison report",
        "",
        f"cache: {cache.cache_dir}",
        f"meetings: {len(first)}",
        f"resolver determinism delta: {len(determinism_delta)}",
        "",
        "## Audited precedence conflict populations",
    ]
    lines.extend(f"- {name}: {count}" for name, count in sorted(comparison.cause_population.items()))
    lines.extend(["", "## Canonical field deltas vs legacy ProjectedMeeting.meeting"])
    lines.extend(f"- {name}: {count}" for name, count in sorted(comparison.field_deltas.items()))
    lines.extend(["", "## Rendered-file deltas vs legacy rendering"])
    lines.extend(f"- {name}: {count}" for name, count in sorted(comparison.file_deltas.items()))
    lines.extend(["", "## State deltas"])
    lines.extend(f"- {name}: {count}" for name, count in sorted(comparison.state_deltas.items()))
    lines.extend([
        "",
        "## Unexpected",
        f"{len(comparison.unexpected)} meetings with unexpected delta",
    ])
    for meeting_id, deltas in sorted(comparison.unexpected.items())[:50]:
        lines.append(f"- {meeting_id}: {', '.join(deltas)}")
    if len(comparison.unexpected) > 50:
        lines.append(f"- ... {len(comparison.unexpected) - 50} more")
    report = "\n".join(lines) + "\n"
    report_path = Path(__file__).with_name("shadow-report.txt")
    report_path.write_text(report)
    print(report, end="")
    if determinism_delta or comparison.unexpected:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
