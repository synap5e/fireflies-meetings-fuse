"""Immutable read-model projection served by the FUSE layer."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal

from .capture import CaptureSnapshot
from .models import AccessLogEntry, Meeting, Sentence, TranscriptDetail
from .renderer import (
    render_meeting_json,
    render_open_script,
    render_participants,
    render_summary,
    render_transcript,
    render_views,
)
from .slug import slugify

MEETING_FILES: tuple[str, ...] = (
    "summary.md",
    "transcript.md",
    "participants.md",
    "meeting.json",
    "open.sh",
    "views.md",
)

CaptureState = Literal["captured", "partial", "live"]
NodeKind = Literal["dir", "file", "symlink"]

SUMMARY_PENDING = b"_Summary pending_\n"
TRANSCRIPT_PENDING = b"_Transcript pending_\n"
ACCESS_LOG_PENDING = b"_Awaiting access log_\n"


@dataclass(frozen=True)
class BackfillDiagnostic:
    last_poll_attempt: str = ""
    last_poll_outcome: str = ""
    backoff_window: str = ""


@dataclass(frozen=True)
class ProjectedNode:
    kind: NodeKind
    children: tuple[tuple[str, bool], ...] = ()
    content: bytes = b""
    target: bytes = b""
    executable: bool = False
    dynamic: bool = False
    meeting_id: str = ""
    file_name: str = ""

    @property
    def size(self) -> int:
        if self.kind == "symlink":
            return len(self.target)
        return len(self.content)


@dataclass(frozen=True)
class ProjectedMeeting:
    meeting: Meeting
    detail: TranscriptDetail
    files: MappingProxyType[str, bytes]
    capture_state: CaptureState
    primary_path: str | None
    live_path: str | None = None
    mine_path: str | None = None
    ghost_id: str | None = None
    overlap_ids: tuple[str, ...] = ()
    overlap_dirnames: MappingProxyType[str, str] = field(
        default_factory=lambda: MappingProxyType({}),
    )
    diagnostic: BackfillDiagnostic = field(default_factory=BackfillDiagnostic)


@dataclass(frozen=True)
class Projection:
    nodes: MappingProxyType[str, ProjectedNode]
    meetings: MappingProxyType[str, ProjectedMeeting]
    live_dirnames: MappingProxyType[str, str]
    user_email: str | None = None
    auth_fatal: bool = False
    chat_auth_fatal: bool = False

    def node(self, path: str) -> ProjectedNode | None:
        return self.nodes.get(path)

    def list_dir(self, path: str) -> tuple[tuple[str, bool], ...]:
        node = self.nodes.get(path)
        if node is None or node.kind != "dir":
            return ()
        return node.children

    def file_content(self, path: str) -> bytes | None:
        node = self.nodes.get(path)
        if node is None or node.kind != "file":
            return None
        return node.content

    def get_meeting_paths(self, meeting_id: str) -> tuple[str, str | None, str | None] | None:
        projected = self.meetings.get(meeting_id)
        if projected is None or projected.primary_path is None:
            return None
        return projected.primary_path, projected.live_path, projected.mine_path

    def is_meeting_dynamic(self, meeting_id: str) -> bool:
        projected = self.meetings.get(meeting_id)
        return projected is not None and projected.capture_state != "captured"


@dataclass(frozen=True)
class ProjectionBuildOptions:
    user_email: str | None = None
    live_captions: dict[str, dict[str, Sentence]] = field(default_factory=dict)
    diagnostics: dict[str, BackfillDiagnostic] = field(default_factory=dict)
    auth_fatal: bool = False
    chat_auth_fatal: bool = False


def empty_projection(*, user_email: str | None = None) -> Projection:
    nodes = MappingProxyType({"/": ProjectedNode(kind="dir")})
    return Projection(
        nodes=nodes,
        meetings=MappingProxyType({}),
        live_dirnames=MappingProxyType({}),
        user_email=user_email,
    )


def build_projection_from_captures(
    snapshot: CaptureSnapshot,
    options: ProjectionBuildOptions | None = None,
) -> Projection:
    opts = options or ProjectionBuildOptions()
    meetings = _meeting_map(snapshot)
    live_rows = opts.live_captions
    diag = opts.diagnostics
    projected_by_id: dict[str, ProjectedMeeting] = {}
    entries_by_date: dict[str, list[Meeting]] = defaultdict(list)

    for meeting in meetings.values():
        if meeting.date_str:
            entries_by_date[meeting.date_str].append(meeting)

    folded_ghosts: dict[str, str] = {}
    folded_overlaps: dict[str, tuple[str, ...]] = {}
    dirname_by_id: dict[str, str] = {}
    hidden_ids: set[str] = set()

    for dated in entries_by_date.values():
        visible, ghosts = _split_ghosts(dated)
        visible, overlaps = _split_overlaps(visible)
        hidden_ids.update(ghosts.values())
        hidden_ids.update(overlap_id for ids in overlaps.values() for overlap_id in ids)
        folded_ghosts.update(ghosts)
        folded_overlaps.update({mid: tuple(ids) for mid, ids in overlaps.items()})
        resolved = _resolve_collisions(visible)
        for dirname, meeting in resolved.items():
            dirname_by_id[meeting.id] = dirname

    for meeting_id, meeting in meetings.items():
        detail_capture = snapshot.details.get(meeting_id)
        access_logs = list(snapshot.access_logs.get(meeting_id, ()))
        has_access_log_capture = meeting_id in snapshot.access_logs
        detail = _project_detail(meeting, detail_capture, access_logs, live_rows.get(meeting_id))
        state = _capture_state(
            meeting,
            detail_capture,
            has_access_log_capture,
            live_rows.get(meeting_id),
        )
        files = MappingProxyType(_render_projected_files(meeting, detail, state, has_access_log_capture))
        primary_path = _primary_path(meeting, dirname_by_id.get(meeting_id)) if meeting_id not in hidden_ids else None
        projected_by_id[meeting_id] = ProjectedMeeting(
            meeting=meeting,
            detail=detail,
            files=files,
            capture_state=state,
            primary_path=primary_path,
            ghost_id=folded_ghosts.get(meeting_id),
            overlap_ids=folded_overlaps.get(meeting_id, ()),
            diagnostic=diag.get(meeting_id, BackfillDiagnostic()),
        )

    live_dirnames = _live_dirnames([m for m in meetings.values() if m.is_live and m.date_str])
    projected_by_id = _attach_secondary_paths(
        projected_by_id,
        dirname_by_id=dirname_by_id,
        live_dirnames=live_dirnames,
        user_email=opts.user_email,
    )
    nodes = _build_nodes(projected_by_id, live_dirnames, user_email=opts.user_email)
    return Projection(
        nodes=MappingProxyType(nodes),
        meetings=MappingProxyType(projected_by_id),
        live_dirnames=MappingProxyType(live_dirnames),
        user_email=opts.user_email,
        auth_fatal=opts.auth_fatal,
        chat_auth_fatal=opts.chat_auth_fatal,
    )


def _meeting_map(snapshot: CaptureSnapshot) -> dict[str, Meeting]:
    meetings = {_with_slug(meeting).id: _with_slug(meeting) for meeting in snapshot.meetings}
    for meeting_id, detail in snapshot.details.items():
        if meeting_id not in meetings:
            meetings[meeting_id] = _with_slug(detail.meeting)
    return meetings


def _make_slug(meeting: Meeting) -> str:
    return slugify(meeting.title) if meeting.title else meeting.id[:12]


def _with_slug(meeting: Meeting) -> Meeting:
    return meeting if meeting.slug else meeting.model_copy(update={"slug": _make_slug(meeting)})


def _project_detail(
    meeting: Meeting,
    detail_capture: TranscriptDetail | None,
    access_logs: list[AccessLogEntry],
    live_rows: dict[str, Sentence] | None,
) -> TranscriptDetail:
    if detail_capture is None:
        detail = TranscriptDetail(meeting=meeting, access_logs=access_logs)
    else:
        detail = detail_capture.model_copy(update={
            "meeting": detail_capture.meeting.model_copy(update={
                "slug": meeting.slug,
                "date_str": meeting.date_str,
                "date_epoch_ms": meeting.date_epoch_ms,
                "is_live": meeting.is_live or detail_capture.meeting.is_live,
            }),
            "access_logs": access_logs,
        })
    if live_rows:
        merged = {str(sentence.index): sentence for sentence in detail.sentences}
        merged.update(live_rows)
        detail = detail.model_copy(update={
            "sentences": sorted(merged.values(), key=lambda sentence: (sentence.start_time, sentence.index)),
            "transcript_error": "",
        })
    return detail


def _capture_state(
    meeting: Meeting,
    detail_capture: TranscriptDetail | None,
    has_access_log_capture: bool,
    live_rows: dict[str, Sentence] | None,
) -> CaptureState:
    if (meeting.is_live or live_rows) and not meeting.summary_is_terminal:
        return "live"
    if detail_capture is None or detail_capture.transcript_error:
        return "partial"
    status = detail_capture.meeting.meeting_info.summary_status or meeting.meeting_info.summary_status
    if status == "missing_from_api":
        return "captured"
    if detail_capture.meeting.is_completed and has_access_log_capture:
        return "captured"
    return "partial"


def _render_projected_files(
    meeting: Meeting,
    detail: TranscriptDetail,
    state: CaptureState,
    has_access_logs: bool,
) -> dict[str, bytes]:
    if state == "captured":
        summary = render_summary(detail.meeting, detail).encode()
        transcript = render_transcript(detail.meeting, detail).encode()
    else:
        summary = SUMMARY_PENDING
        transcript = TRANSCRIPT_PENDING if not detail.sentences else render_transcript(detail.meeting, detail).encode()
    views = render_views(detail.meeting, detail).encode() if has_access_logs else ACCESS_LOG_PENDING
    return {
        "summary.md": summary,
        "transcript.md": transcript,
        "participants.md": render_participants(detail.meeting, detail).encode(),
        "meeting.json": _render_meeting_json_with_state(detail, state).encode(),
        "open.sh": render_open_script(meeting).encode(),
        "views.md": views,
    }


def _render_meeting_json_with_state(detail: TranscriptDetail, state: CaptureState) -> str:
    raw = json.loads(render_meeting_json(detail.meeting, detail))
    if isinstance(raw, dict):
        raw["capture_state"] = state
    return json.dumps(raw, indent=2, ensure_ascii=False) + "\n"


def _resolve_collisions(meetings: list[Meeting]) -> dict[str, Meeting]:
    result: dict[str, Meeting] = {}
    slug_count: dict[str, int] = {}
    for meeting in sorted(meetings, key=lambda item: item.date_epoch_ms):
        count = slug_count.get(meeting.slug, 0)
        slug_count[meeting.slug] = count + 1
        dirname = meeting.slug if count == 0 else f"{meeting.slug}-{count + 1}"
        result[dirname] = meeting
    return result


def _split_ghosts(meetings: list[Meeting]) -> tuple[list[Meeting], dict[str, str]]:
    groups: dict[str, list[Meeting]] = defaultdict(list)
    for meeting in meetings:
        groups[meeting.slug].append(meeting)
    folded: set[str] = set()
    ghost_map: dict[str, str] = {}
    for group in groups.values():
        if len(group) != 2:
            continue
        ghosts = [m for m in group if m.duration_mins == 0 and m.summary_is_terminal]
        real = [m for m in group if m.duration_mins > 0]
        if len(ghosts) == 1 and real:
            primary = min(real, key=lambda item: item.date_epoch_ms)
            folded.add(ghosts[0].id)
            ghost_map[primary.id] = ghosts[0].id
    return [meeting for meeting in meetings if meeting.id not in folded], ghost_map


def _split_overlaps(meetings: list[Meeting]) -> tuple[list[Meeting], dict[str, list[str]]]:
    groups: dict[tuple[str, str], list[Meeting]] = defaultdict(list)
    for meeting in meetings:
        groups[(meeting.slug, meeting.date_str)].append(meeting)
    folded: set[str] = set()
    overlap_map: dict[str, list[str]] = {}
    for group in groups.values():
        if len(group) < 2 or any(meeting.duration_mins <= 0 for meeting in group):
            continue
        primary = min(group, key=lambda item: (-item.duration_mins, item.date_epoch_ms))
        overlaps = [
            meeting for meeting in sorted(group, key=lambda item: item.date_epoch_ms)
            if meeting.id != primary.id and _windows_overlap(primary, meeting)
        ]
        if overlaps:
            folded.update(meeting.id for meeting in overlaps)
            overlap_map[primary.id] = [meeting.id for meeting in overlaps]
    return [meeting for meeting in meetings if meeting.id not in folded], overlap_map


def _windows_overlap(a: Meeting, b: Meeting) -> bool:
    a_start = a.date_epoch_ms
    a_end = a_start + a.duration_mins * 60_000
    b_start = b.date_epoch_ms
    b_end = b_start + b.duration_mins * 60_000
    return max(a_start, b_start) < min(a_end, b_end)


def _primary_path(meeting: Meeting, dirname: str | None) -> str | None:
    if dirname is None or not meeting.date_str:
        return None
    return f"/{meeting.date_str[:7]}/{meeting.date_str[8:10]}/{dirname}"


def _live_dirnames(live_meetings: list[Meeting]) -> dict[str, str]:
    return {dirname: meeting.id for dirname, meeting in _resolve_collisions(live_meetings).items()}


def _attach_secondary_paths(
    projected: dict[str, ProjectedMeeting],
    *,
    dirname_by_id: dict[str, str],
    live_dirnames: dict[str, str],
    user_email: str | None,
) -> dict[str, ProjectedMeeting]:
    mine_dirname_by_id = _mine_dirnames(projected, user_email)
    result: dict[str, ProjectedMeeting] = {}
    live_path_by_id = {meeting_id: f"/live/{dirname}" for dirname, meeting_id in live_dirnames.items()}
    for meeting_id, item in projected.items():
        mine_path = None
        meeting = item.meeting
        mine_dirname = mine_dirname_by_id.get(meeting_id)
        if mine_dirname is not None and meeting.date_str:
            mine_path = f"/mine/{meeting.date_str[:7]}/{meeting.date_str[8:10]}/{mine_dirname}"
        overlap_dirnames = MappingProxyType({
            _overlap_dirname(index): overlap_id
            for index, overlap_id in enumerate(item.overlap_ids)
        })
        primary_path = _primary_path(meeting, dirname_by_id.get(meeting_id))
        result[meeting_id] = ProjectedMeeting(
            meeting=item.meeting,
            detail=item.detail,
            files=item.files,
            capture_state=item.capture_state,
            primary_path=primary_path,
            live_path=live_path_by_id.get(meeting_id),
            mine_path=mine_path,
            ghost_id=item.ghost_id,
            overlap_ids=item.overlap_ids,
            overlap_dirnames=overlap_dirnames,
            diagnostic=item.diagnostic,
        )
    return result


def _mine_dirnames(
    projected: dict[str, ProjectedMeeting],
    user_email: str | None,
) -> dict[str, str]:
    if not user_email:
        return {}
    by_date: dict[str, list[Meeting]] = defaultdict(list)
    hidden = {
        hidden_id
        for item in projected.values()
        for hidden_id in ((*item.overlap_ids,), item.ghost_id)
        if hidden_id is not None
    }
    for item in projected.values():
        meeting = item.meeting
        if meeting.id not in hidden and meeting.organizer_email == user_email and meeting.date_str:
            by_date[meeting.date_str].append(meeting)
    result: dict[str, str] = {}
    for meetings in by_date.values():
        for dirname, meeting in _resolve_collisions(meetings).items():
            result[meeting.id] = dirname
    return result


def _build_nodes(
    meetings: dict[str, ProjectedMeeting],
    live_dirnames: dict[str, str],
    *,
    user_email: str | None,
) -> dict[str, ProjectedNode]:
    children: dict[str, dict[str, bool]] = defaultdict(dict)
    files: dict[str, ProjectedNode] = {}
    symlinks: dict[str, ProjectedNode] = {}

    def add_dir(path: str) -> None:
        children.setdefault(path, {})
        if path == "/":
            return
        parent, name = path.rsplit("/", 1)
        if parent and parent not in children:
            add_dir(parent)
        children.setdefault(parent or "/", {})
        children[parent or "/"][name] = True

    def add_file(path: str, content: bytes, meeting_id: str = "", file_name: str = "") -> None:
        parent, name = path.rsplit("/", 1)
        add_dir(parent or "/")
        children[parent or "/"][name] = False
        files[path] = ProjectedNode(
            kind="file",
            content=content,
            executable=name == "open.sh",
            dynamic=meeting_id != "" and meetings[meeting_id].capture_state != "captured",
            meeting_id=meeting_id,
            file_name=file_name,
        )

    add_dir("/")
    add_dir("/live")
    if user_email:
        add_dir("/mine")

    for item in meetings.values():
        _add_projected_paths(add_dir, add_file, meetings, item)

    for dirname, meeting_id in live_dirnames.items():
        item = meetings.get(meeting_id)
        if item is None or item.primary_path is None:
            continue
        path = f"/live/{dirname}"
        children["/live"][dirname] = False
        symlinks[path] = ProjectedNode(
            kind="symlink",
            target=f"..{item.primary_path}".encode(),
            dynamic=True,
            meeting_id=meeting_id,
        )

    add_file("/BACKFILL_IN_PROGRESS", _render_backfill_summary(meetings))

    nodes: dict[str, ProjectedNode] = {}
    for path, child_map in children.items():
        nodes[path] = ProjectedNode(
            kind="dir",
            children=tuple(sorted(child_map.items(), reverse=True)),
        )
    nodes.update(files)
    nodes.update(symlinks)
    return nodes


def _add_projected_paths(
    add_dir: Callable[[str], None],
    add_file: Callable[[str, bytes, str, str], None],
    meetings: dict[str, ProjectedMeeting],
    item: ProjectedMeeting,
) -> None:
    for base_path in (item.primary_path, item.mine_path):
        if base_path is None:
            continue
        _add_meeting_tree(add_dir, add_file, base_path, item)
        _add_folded_meeting_files(add_file, meetings, item, base_path)


def _add_folded_meeting_files(
    add_file: Callable[[str, bytes, str, str], None],
    meetings: dict[str, ProjectedMeeting],
    item: ProjectedMeeting,
    base_path: str,
) -> None:
    if item.ghost_id is not None:
        ghost = meetings.get(item.ghost_id)
        if ghost is not None:
            _add_files_under(add_file, f"{base_path}/ghost", ghost)
    if item.overlap_dirnames:
        add_file(
            f"{base_path}/_overlap_warning.md",
            _render_overlap_warning(item, meetings),
            item.meeting.id,
            "_overlap_warning.md",
        )
    for dirname, overlap_id in item.overlap_dirnames.items():
        overlap = meetings.get(overlap_id)
        if overlap is not None:
            _add_files_under(add_file, f"{base_path}/{dirname}", overlap)


def _add_files_under(
    add_file: Callable[[str, bytes, str, str], None],
    base_path: str,
    item: ProjectedMeeting,
) -> None:
    for filename in MEETING_FILES:
        add_file(f"{base_path}/{filename}", item.files[filename], item.meeting.id, filename)


def _add_meeting_tree(
    add_dir: Callable[[str], None],
    add_file: Callable[[str, bytes, str, str], None],
    meeting_path: str,
    item: ProjectedMeeting,
) -> None:
    year_month_path, _day, _slug = meeting_path.rsplit("/", 2)
    month_path = "/" + meeting_path.strip("/").split("/")[0]
    add_dir(month_path)
    add_dir(year_month_path)
    add_dir(meeting_path)
    for filename in MEETING_FILES:
        add_file(f"{meeting_path}/{filename}", item.files[filename], item.meeting.id, filename)
    if item.ghost_id is not None:
        add_dir(f"{meeting_path}/ghost")
        # Filled by the primary build pass once all meetings are available.
    for dirname in item.overlap_dirnames:
        add_dir(f"{meeting_path}/{dirname}")


def _render_backfill_summary(meetings: dict[str, ProjectedMeeting]) -> bytes:
    partial = [
        item for item in meetings.values()
        if item.capture_state != "captured" and item.primary_path is not None
    ]
    if not partial:
        return b"# Backfill In Progress\n\n_No meetings currently need backfill._\n"
    parts = ["# Backfill In Progress", ""]
    for item in sorted(partial, key=lambda p: (p.meeting.date_str, p.meeting.slug), reverse=True):
        diag = item.diagnostic
        parts.extend([
            f"## {item.meeting.date_str} {item.meeting.slug} ({item.meeting.id})",
            "",
            f"- capture_state: {item.capture_state}",
            f"- last_poll_attempt: {diag.last_poll_attempt or 'never'}",
            f"- last_poll_outcome: {diag.last_poll_outcome or 'not attempted'}",
            f"- backoff_window: {diag.backoff_window or 'none'}",
            "",
        ])
    return "\n".join(parts).encode()


def _render_overlap_warning(
    primary: ProjectedMeeting,
    meetings: dict[str, ProjectedMeeting],
) -> bytes:
    primary_texts = {
        normalized
        for sentence in primary.detail.sentences
        if (normalized := _normalize_sentence_text(sentence.text))
    }
    missing_by_overlap: list[tuple[str, str, list[Sentence]]] = []
    for dirname, overlap_id in primary.overlap_dirnames.items():
        overlap = meetings.get(overlap_id)
        if overlap is None:
            continue
        missing = [
            sentence
            for sentence in overlap.detail.sentences
            if (normalized := _normalize_sentence_text(sentence.text))
            and normalized not in primary_texts
        ]
        if missing:
            missing_by_overlap.append((dirname, overlap_id, missing))

    if not missing_by_overlap:
        return (
            b"---\nwarning: overlap-superset\n---\n\n"
            b"# Overlap Warning\n\n"
            b"No missing sentences were found. The primary recording appears "
            b"to be a strict superset of all overlap recordings.\n"
        )

    parts = [
        "---",
        "warning: overlap-not-superset",
        "---",
        "",
        "# Overlap Warning",
        "",
        "The primary recording is NOT a strict superset of all overlap recordings.",
        "The following sentences appear in an overlap recording but not in this one:",
        "",
    ]
    for dirname, overlap_id, missing_sentences in missing_by_overlap:
        parts.extend([
            f"## From `{dirname}/` (ID: {overlap_id})",
            "",
            "| Time | Speaker | Text |",
            "|------|---------|------|",
        ])
        for sentence in missing_sentences:
            parts.append(
                "| "
                f"{_format_sentence_time(sentence)} | "
                f"{_markdown_table_cell(sentence.speaker_name)} | "
                f"{_markdown_table_cell(sentence.text)} |"
            )
        parts.append("")
    return "\n".join(parts).encode()


def _normalize_sentence_text(text: str) -> str:
    return text.strip().casefold()


def _format_sentence_time(sentence: Sentence) -> str:
    offset_sec = max(0, int(sentence.start_time))
    minutes = offset_sec // 60
    seconds = offset_sec % 60
    return f"{minutes}:{seconds:02d}"


def _markdown_table_cell(text: str) -> str:
    return text.replace("\n", " ").replace("|", "\\|").strip()


def _overlap_dirname(index: int) -> str:
    return "overlap" if index == 0 else f"overlap-{index + 1}"
