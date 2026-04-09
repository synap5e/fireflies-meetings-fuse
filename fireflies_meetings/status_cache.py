"""Persistent cache for meeting completion status.

Stores empty marker files in XDG_CACHE_HOME/fireflies-meetings/completed/
so we know which meetings are fully processed and don't need re-fetching.
"""

from __future__ import annotations

import os
from pathlib import Path


class StatusCache:
    """Tracks which meetings have completed processing via filesystem markers."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        self._dir = cache_dir or (
            Path(os.environ.get("XDG_CACHE_HOME", "~/.cache")).expanduser()
            / "fireflies-meetings"
            / "completed"
        )
        self._dir.mkdir(parents=True, exist_ok=True)

    def is_completed(self, meeting_id: str) -> bool:
        return (self._dir / meeting_id).exists()

    def mark_completed(self, meeting_id: str) -> None:
        (self._dir / meeting_id).touch()

    @property
    def cache_dir(self) -> Path:
        return self._dir.parent
