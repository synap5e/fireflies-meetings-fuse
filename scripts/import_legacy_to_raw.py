#!/usr/bin/env python3
"""Import latest-wins capture files into the append-only raw archive."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from fireflies_meetings.capture import default_cache_dir
from fireflies_meetings.raw import LegacyImportSink, RawEnvelope, canonicalize_body

_STATE_FILENAME = "_legacy_import_state.json"


@dataclass(frozen=True)
class ImportResult:
    imported: int
    skipped: int


@dataclass(frozen=True)
class _ImportCandidate:
    path: Path
    source: str
    endpoint: str
    operation_variables: object | None = None


def _candidates(cache_root: Path) -> list[_ImportCandidate]:
    candidates = [
        _ImportCandidate(cache_root / "list.json", "fireflies-public-graphql", "transcripts"),
        _ImportCandidate(cache_root / "channels.json", "fireflies-hive", "channels"),
    ]
    meetings_root = cache_root / "meetings"
    if not meetings_root.is_dir():
        return candidates
    for meeting_dir in sorted(meetings_root.iterdir()):
        if not meeting_dir.is_dir():
            continue
        variables = {"id": meeting_dir.name}
        candidates.extend([
            _ImportCandidate(
                meeting_dir / "detail.json",
                "fireflies-public-graphql",
                "transcript",
                variables,
            ),
            _ImportCandidate(
                meeting_dir / "access_logs.json",
                "fireflies-hive",
                "access-logs",
                variables,
            ),
        ])
    return candidates


def _load_state(path: Path) -> set[str]:
    try:
        raw: object = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(raw, dict):
        return set()
    imported = cast("dict[str, object]", raw).get("imported")
    if not isinstance(imported, list):
        return set()
    return {item for item in cast("list[object]", imported) if isinstance(item, str)}


def _write_state(path: Path, imported: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(canonicalize_body({"version": 1, "imported": sorted(imported)}) + "\n")
    os.replace(tmp, path)


def _signature(cache_root: Path, path: Path) -> str:
    stat = path.stat()
    return f"{path.relative_to(cache_root)}:{stat.st_mtime_ns}:{stat.st_size}"


def import_legacy(cache_root: Path, *, raw_root: Path | None = None) -> ImportResult:
    """Import all recognized legacy captures, resuming from durable state."""
    destination = raw_root or cache_root / "raw"
    sink = LegacyImportSink(destination)
    state_path = destination / _STATE_FILENAME
    imported_signatures = _load_state(state_path)
    imported_count = 0
    skipped_count = 0

    for candidate in _candidates(cache_root):
        if not candidate.path.is_file():
            skipped_count += 1
            continue
        signature = _signature(cache_root, candidate.path)
        if signature in imported_signatures:
            skipped_count += 1
            continue
        try:
            body: object = json.loads(candidate.path.read_text())
        except (OSError, json.JSONDecodeError):
            skipped_count += 1
            continue
        sink.write(
            RawEnvelope(
                source=candidate.source,
                endpoint=candidate.endpoint,
                operation_variables=candidate.operation_variables,
                page_cursor=None,
                fetched_at=candidate.path.stat().st_mtime,
                outcome="legacy-import",
                body_encoding="json",
                body=body,
            )
        )
        imported_signatures.add(signature)
        _write_state(state_path, imported_signatures)
        imported_count += 1

    return ImportResult(imported=imported_count, skipped=skipped_count)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=default_cache_dir(),
        help="Existing cache root (default: XDG fireflies-meetings cache)",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        help="Raw archive destination (default: <cache-root>/raw)",
    )
    args = parser.parse_args()
    result = import_legacy(args.cache_root.expanduser(), raw_root=args.raw_root)
    print(f"Imported {result.imported} legacy captures; skipped {result.skipped}.")


if __name__ == "__main__":
    main()
