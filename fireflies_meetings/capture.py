"""Raw capture storage for Fireflies meetings.

The capture layer persists API facts only. Rendered markdown and FUSE path
shape are derived later by :mod:`fireflies_meetings.projection`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from pydantic import ValidationError

from .api import JsonObject
from .models import AccessLogEntry, Meeting, TranscriptDetail

log = logging.getLogger(__name__)

_LEGACY_DETAIL_PATTERN = re.compile(r"^detail\.legacy\.(\d{14})$")
_LEGACY_RETENTION = timedelta(days=7)


def default_cache_dir() -> Path:
    return Path(os.environ.get("XDG_CACHE_HOME", "~/.cache")).expanduser() / "fireflies-meetings"


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _dump_json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=2).encode() + b"\n"


@dataclass(frozen=True)
class CaptureSnapshot:
    meetings: tuple[Meeting, ...]
    details: dict[str, TranscriptDetail]
    access_logs: dict[str, tuple[AccessLogEntry, ...]]


class CaptureStore:
    """Filesystem-backed raw capture store."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        self.cache_dir = cache_dir or default_cache_dir()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        migrate_legacy_cache(self.cache_dir)

    @property
    def list_path(self) -> Path:
        return self.cache_dir / "list.json"

    @property
    def meetings_dir(self) -> Path:
        return self.cache_dir / "meetings"

    def detail_path(self, meeting_id: str) -> Path:
        return self.meetings_dir / meeting_id / "detail.json"

    def access_logs_path(self, meeting_id: str) -> Path:
        return self.meetings_dir / meeting_id / "access_logs.json"

    def read_snapshot(self) -> CaptureSnapshot:
        return CaptureSnapshot(
            meetings=tuple(self.read_list()),
            details=self.read_details(),
            access_logs=self.read_access_logs(),
        )

    def read_list(self) -> list[Meeting]:
        try:
            raw: object = json.loads(self.list_path.read_text())
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(raw, dict):
            return []
        typed = cast("JsonObject", raw)
        raw_meetings = typed.get("meetings")
        if not isinstance(raw_meetings, list):
            return []
        meetings: list[Meeting] = []
        for item in raw_meetings:
            if not isinstance(item, dict):
                continue
            try:
                meetings.append(Meeting.model_validate(item))
            except ValidationError as e:
                log.warning("Skipping malformed captured meeting: %s", e)
        return meetings

    def write_list(self, meetings: list[Meeting], *, fetched_at: float) -> None:
        atomic_write_bytes(
            self.list_path,
            _dump_json_bytes({
                "v": 1,
                "fetched_at": fetched_at,
                "meetings": [meeting.model_dump() for meeting in meetings],
            }),
        )

    def read_details(self) -> dict[str, TranscriptDetail]:
        details: dict[str, TranscriptDetail] = {}
        if not self.meetings_dir.is_dir():
            return details
        for meeting_dir in self.meetings_dir.iterdir():
            if not meeting_dir.is_dir():
                continue
            path = meeting_dir / "detail.json"
            if not path.is_file():
                continue
            try:
                details[meeting_dir.name] = TranscriptDetail.model_validate_json(path.read_text())
            except (OSError, ValidationError) as e:
                log.warning("Skipping malformed detail capture %s: %s", path, e)
        return details

    def write_detail(self, meeting_id: str, detail: TranscriptDetail) -> None:
        atomic_write_bytes(self.detail_path(meeting_id), detail.model_dump_json(indent=2).encode() + b"\n")

    def read_access_logs(self) -> dict[str, tuple[AccessLogEntry, ...]]:
        logs: dict[str, tuple[AccessLogEntry, ...]] = {}
        if not self.meetings_dir.is_dir():
            return logs
        for meeting_dir in self.meetings_dir.iterdir():
            if not meeting_dir.is_dir():
                continue
            path = meeting_dir / "access_logs.json"
            if not path.is_file():
                continue
            try:
                raw: object = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError) as e:
                log.warning("Skipping malformed access-log capture %s: %s", path, e)
                continue
            if not isinstance(raw, list):
                continue
            entries: list[AccessLogEntry] = []
            for item in cast("list[object]", raw):
                if not isinstance(item, dict):
                    continue
                try:
                    entries.append(AccessLogEntry.model_validate(item))
                except ValidationError as e:
                    log.debug("Skipping malformed access-log row in %s: %s", path, e)
            logs[meeting_dir.name] = tuple(entries)
        return logs

    def write_access_logs(self, meeting_id: str, logs: list[AccessLogEntry]) -> None:
        atomic_write_bytes(
            self.access_logs_path(meeting_id),
            _dump_json_bytes([entry.model_dump() for entry in logs]),
        )


def migrate_legacy_cache(cache_dir: Path) -> None:
    """Convert legacy ``detail/<id>/meeting.json`` cache into raw captures.

    Migration is staged and verified before the old ``detail`` directory is
    renamed aside. No API calls are made.
    """
    _purge_old_legacy_dirs(cache_dir)
    legacy_detail = cache_dir / "detail"
    meetings_dir = cache_dir / "meetings"
    if not legacy_detail.is_dir() or meetings_dir.exists():
        return

    staging = cache_dir / "meetings.staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    try:
        migrated = 0
        for detail_dir in legacy_detail.iterdir():
            if not detail_dir.is_dir():
                continue
            meeting_json = detail_dir / "meeting.json"
            if not meeting_json.is_file():
                continue
            detail, logs = _legacy_meeting_json_to_captures(meeting_json)
            meeting_id = detail.meeting.id or detail_dir.name
            target = staging / meeting_id
            atomic_write_bytes(
                target / "detail.json",
                detail.model_dump_json(indent=2).encode() + b"\n",
            )
            atomic_write_bytes(
                target / "access_logs.json",
                _dump_json_bytes([entry.model_dump() for entry in logs]),
            )
            migrated += 1

        _verify_staging(staging)
        os.replace(staging, meetings_dir)
        timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        os.replace(legacy_detail, cache_dir / f"detail.legacy.{timestamp}")
        log.info("Migrated %d legacy meeting caches to raw capture layout", migrated)
    except (OSError, ValidationError, json.JSONDecodeError, ValueError):
        log.exception("Legacy cache migration failed; leaving detail/ intact")
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def _legacy_meeting_json_to_captures(path: Path) -> tuple[TranscriptDetail, list[AccessLogEntry]]:
    raw: object = json.loads(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{path} is not a JSON object")
    data = cast("dict[str, object]", raw)
    meeting_data = {
        "id": data.get("id"),
        "title": data.get("title"),
        "date_epoch_ms": data.get("date_epoch_ms"),
        "date_str": data.get("date"),
        "duration_mins": data.get("duration_mins"),
        "is_live": data.get("is_live"),
        "organizer_email": data.get("organizer_email"),
        "participants": data.get("participants"),
        "transcript_url": data.get("transcript_url"),
        "meeting_info": data.get("meeting_info"),
        "slug": data.get("slug"),
    }
    meeting = Meeting.model_validate(meeting_data)
    detail = TranscriptDetail.model_validate({
        "meeting": meeting.model_dump(),
        "sentences": data.get("transcript") or data.get("sentences") or [],
        "summary": data.get("summary"),
        "speakers": data.get("speakers") or [],
        "attendees": data.get("attendees") or [],
        "transcript_error": data.get("transcript_error") or "",
    })
    logs: list[AccessLogEntry] = []
    raw_logs = data.get("access_logs")
    if isinstance(raw_logs, list):
        for item in cast("list[object]", raw_logs):
            if isinstance(item, dict):
                logs.append(AccessLogEntry.model_validate(item))
    return detail, logs


def _verify_staging(staging: Path) -> None:
    for meeting_dir in staging.iterdir():
        if not meeting_dir.is_dir():
            continue
        TranscriptDetail.model_validate_json((meeting_dir / "detail.json").read_text())
        raw: object = json.loads((meeting_dir / "access_logs.json").read_text())
        if not isinstance(raw, list):
            raise ValueError(f"{meeting_dir / 'access_logs.json'} is not a list")
        for item in cast("list[object]", raw):
            AccessLogEntry.model_validate(item)


def _purge_old_legacy_dirs(cache_dir: Path) -> None:
    now = datetime.now(UTC)
    for child in cache_dir.iterdir() if cache_dir.exists() else ():
        if not child.is_dir():
            continue
        match = _LEGACY_DETAIL_PATTERN.match(child.name)
        if match is None:
            continue
        stamp = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        if now - stamp > _LEGACY_RETENTION:
            shutil.rmtree(child, ignore_errors=True)
