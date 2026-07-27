"""Tests for append-only raw observation storage and legacy import."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from fireflies_meetings.raw import JsonlRawSink, RawEnvelope, canonicalize_body
from scripts.import_legacy_to_raw import import_legacy


def _envelope(
    *,
    fetched_at: float = 1785283200.0,
    body: object = {"data": {"transcripts": []}},
    source: str = "fireflies-public-graphql",
    endpoint: str = "transcripts",
) -> RawEnvelope:
    return RawEnvelope(
        source=source,
        endpoint=endpoint,
        operation_variables={"limit": 50, "skip": 0},
        page_cursor={"page": 1, "page_size": 50},
        fetched_at=fetched_at,
        outcome="ok",
        body_encoding="json",
        body=body,
    )


def _read_lines(path: Path) -> list[dict[str, object]]:
    return [cast("dict[str, object]", json.loads(line)) for line in path.read_text().splitlines() if line]


def test_envelope_round_trip_and_canonical_body(tmp_path: Path) -> None:
    sink = JsonlRawSink(tmp_path)
    envelope = _envelope(body={"z": [3, 2], "a": {"value": True}})
    sink.write(envelope)

    path = tmp_path / envelope.source / "2026-07-29.jsonl"
    [raw] = _read_lines(path)
    assert RawEnvelope.from_dict(raw) == envelope
    assert canonicalize_body({"b": 2, "a": 1}) == '{"a":1,"b":2}'


def test_identical_last_value_is_deduplicated_across_sink_restarts(tmp_path: Path) -> None:
    envelope = _envelope()
    JsonlRawSink(tmp_path).write(envelope)
    JsonlRawSink(tmp_path).write(replace(envelope, fetched_at=envelope.fetched_at + 1))

    path = tmp_path / envelope.source / "2026-07-29.jsonl"
    assert len(_read_lines(path)) == 1


def test_dedup_skip_still_updates_last_seen(tmp_path: Path) -> None:
    sink = JsonlRawSink(tmp_path)
    sink.write(_envelope(fetched_at=1785283200.0))
    sink.write(_envelope(fetched_at=1785283260.0))

    last_seen_path = tmp_path / "_last_seen" / "fireflies-public-graphql" / "transcripts.json"
    last_seen = cast("dict[str, object]", json.loads(last_seen_path.read_text()))
    assert last_seen == {"fetched_at": 1785283260.0, "outcome": "ok"}


def test_daily_rotation_uses_fetched_at_utc_date(tmp_path: Path) -> None:
    sink = JsonlRawSink(tmp_path)
    before_midnight = datetime(2026, 7, 28, 23, 59, 59, tzinfo=UTC).timestamp()
    after_midnight = datetime(2026, 7, 29, 0, 0, 1, tzinfo=UTC).timestamp()

    sink.write(_envelope(fetched_at=before_midnight, body={"version": 1}))
    sink.write(_envelope(fetched_at=after_midnight, body={"version": 2}))

    source_dir = tmp_path / "fireflies-public-graphql"
    assert len(_read_lines(source_dir / "2026-07-28.jsonl")) == 1
    assert len(_read_lines(source_dir / "2026-07-29.jsonl")) == 1


@pytest.mark.parametrize(
    ("source", "endpoint"),
    [
        ("../escape", "transcripts"),
        ("Fireflies", "transcripts"),
        ("fireflies-public-graphql", "meeting/id"),
        ("fireflies-public-graphql", "has_underscore"),
    ],
)
def test_path_components_are_restricted(tmp_path: Path, source: str, endpoint: str) -> None:
    with pytest.raises(ValueError, match=r"\[a-z0-9-\]\+"):
        JsonlRawSink(tmp_path).write(_envelope(source=source, endpoint=endpoint))


def test_concurrent_writes_are_serialized_without_corrupting_jsonl(tmp_path: Path) -> None:
    sink = JsonlRawSink(tmp_path)

    def write(index: int) -> None:
        sink.write(_envelope(body={"index": index}))

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(write, range(100)))

    lines = _read_lines(tmp_path / "fireflies-public-graphql" / "2026-07-29.jsonl")
    assert len(lines) == 100
    assert {cast("dict[str, int]", line["body"])["index"] for line in lines} == set(range(100))


def test_legacy_import_is_tagged_and_idempotent(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    meeting_dir = cache / "meetings" / "MEET01"
    meeting_dir.mkdir(parents=True)
    (cache / "list.json").write_text('{"meetings":[{"id":"MEET01"}]}')
    (cache / "channels.json").write_text('{"channels":[],"memberships":{}}')
    (meeting_dir / "detail.json").write_text('{"meeting":{"id":"MEET01"}}')
    (meeting_dir / "access_logs.json").write_text('{"outcome":"ok","logs":[]}')

    first = import_legacy(cache)
    second = import_legacy(cache)

    assert first.imported == 4
    assert first.skipped == 0
    assert second.imported == 0
    assert second.skipped == 4
    envelopes = [
        envelope
        for source_dir in (cache / "raw").iterdir()
        if source_dir.is_dir() and not source_dir.name.startswith("_")
        for path in source_dir.glob("*.jsonl")
        for envelope in _read_lines(path)
    ]
    assert len(envelopes) == 4
    assert {envelope["outcome"] for envelope in envelopes} == {"legacy-import"}
    assert any(envelope["body"] == {"outcome": "ok", "logs": []} for envelope in envelopes)
