"""Append-only archive of observations received at external boundaries."""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

SCHEMA_VERSION = "2026-07"
_PATH_COMPONENT = re.compile(r"^[a-z0-9-]+$")


def canonicalize_body(body: object) -> str:
    """Serialize JSON deterministically for envelopes and deduplication."""
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True)
class RawEnvelope:
    """Versioned representation of one external or synthetic observation."""

    source: str
    endpoint: str
    fetched_at: float
    outcome: str
    body: object
    operation_variables: object | None = None
    page_cursor: object | None = None
    schema_version: str = SCHEMA_VERSION
    body_encoding: str = "json"
    envelope_version: int = 1

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: object) -> RawEnvelope:
        if not isinstance(raw, dict):
            raise ValueError("Raw envelope must be a JSON object")
        data = cast("dict[str, object]", raw)
        source = data.get("source")
        endpoint = data.get("endpoint")
        fetched_at = data.get("fetched_at")
        outcome = data.get("outcome")
        schema_version = data.get("schema_version")
        body_encoding = data.get("body_encoding")
        envelope_version = data.get("envelope_version")
        if not isinstance(source, str) or not isinstance(endpoint, str):
            raise ValueError("Raw envelope source and endpoint must be strings")
        if not isinstance(fetched_at, (int, float)) or not isinstance(outcome, str):
            raise ValueError("Raw envelope fetched_at/outcome are invalid")
        if not isinstance(schema_version, str) or not isinstance(body_encoding, str):
            raise ValueError("Raw envelope schema/body encoding are invalid")
        if not isinstance(envelope_version, int):
            raise ValueError("Raw envelope version must be an integer")
        return cls(
            source=source,
            endpoint=endpoint,
            operation_variables=data.get("operation_variables"),
            page_cursor=data.get("page_cursor"),
            fetched_at=float(fetched_at),
            outcome=outcome,
            schema_version=schema_version,
            body_encoding=body_encoding,
            body=data.get("body"),
            envelope_version=envelope_version,
        )


class RawSink(Protocol):
    """Destination for immutable raw envelopes."""

    def write(self, envelope: RawEnvelope) -> None:
        """Persist an envelope, or raise if persistence fails."""
        ...


class NoOpRawSink:
    """Raw sink used where archival is intentionally disabled."""

    def write(self, envelope: RawEnvelope) -> None:
        del envelope


class JsonlRawSink:
    """Append envelopes to UTC-daily JSONL files with last-value dedup."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._locks_guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._last_hashes: dict[str, dict[str, str]] = {}

    def _source_lock(self, source: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(source, threading.Lock())

    @staticmethod
    def _validate_component(value: str, *, field_name: str) -> None:
        if not _PATH_COMPONENT.fullmatch(value):
            raise ValueError(f"{field_name} must match [a-z0-9-]+: {value!r}")

    @staticmethod
    def _dedup_key(envelope: RawEnvelope) -> str:
        return canonicalize_body({
            "source": envelope.source,
            "endpoint": envelope.endpoint,
            "operation_variables": envelope.operation_variables,
            "page_cursor": envelope.page_cursor,
            "schema_version": envelope.schema_version,
        })

    @classmethod
    def _dedup_hash(cls, envelope: RawEnvelope) -> str:
        material = canonicalize_body({
            "key": cls._dedup_key(envelope),
            "canonical_body": canonicalize_body(envelope.body),
        })
        return hashlib.sha256(material.encode()).hexdigest()

    def _dedup_state_path(self, source: str) -> Path:
        return self.root / "_dedup" / f"{source}.json"

    def _load_source_hashes(self, source: str) -> dict[str, str]:
        cached = self._last_hashes.get(source)
        if cached is not None:
            return cached
        path = self._dedup_state_path(source)
        try:
            raw: object = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            hashes: dict[str, str] = {}
        else:
            hashes = (
                {
                    str(key): str(value)
                    for key, value in cast("dict[object, object]", raw).items()
                }
                if isinstance(raw, dict)
                else {}
            )
        self._last_hashes[source] = hashes
        return hashes

    @staticmethod
    def _atomic_write_json(path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(canonicalize_body(value) + "\n")
        os.replace(tmp, path)

    def _write_last_seen(self, envelope: RawEnvelope) -> None:
        path = self.root / "_last_seen" / envelope.source / f"{envelope.endpoint}.json"
        self._atomic_write_json(
            path,
            {
                "fetched_at": envelope.fetched_at,
                "outcome": envelope.outcome,
            },
        )

    def _append(self, envelope: RawEnvelope) -> None:
        day = datetime.fromtimestamp(envelope.fetched_at, tz=UTC).date().isoformat()
        path = self.root / envelope.source / f"{day}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as output:
            output.write(canonicalize_body(envelope.to_dict()) + "\n")
            output.flush()

    def _should_deduplicate(self, envelope: RawEnvelope) -> bool:
        del envelope
        return True

    def write(self, envelope: RawEnvelope) -> None:
        self._validate_component(envelope.source, field_name="source")
        self._validate_component(envelope.endpoint, field_name="endpoint")
        lock = self._source_lock(envelope.source)
        with lock:
            self._write_last_seen(envelope)
            if not self._should_deduplicate(envelope):
                self._append(envelope)
                return

            key = self._dedup_key(envelope)
            digest = self._dedup_hash(envelope)
            hashes = self._load_source_hashes(envelope.source)
            if hashes.get(key) == digest:
                return

            self._append(envelope)
            hashes[key] = digest
            self._atomic_write_json(self._dedup_state_path(envelope.source), hashes)


class LegacyImportSink(JsonlRawSink):
    """Archive reconstructed legacy captures without deduplicating them."""

    def _should_deduplicate(self, envelope: RawEnvelope) -> bool:
        if envelope.outcome != "legacy-import":
            raise ValueError("LegacyImportSink requires outcome='legacy-import'")
        return False
