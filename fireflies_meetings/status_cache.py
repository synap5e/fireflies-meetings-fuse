"""Single source of truth for meeting completion status.

A meeting is "completed" if and only if its detail cache has a `.complete`
sentinel — the same file `_save_detail_to_disk` writes last after the
six rendered bytes are in place. The sentinel doubles as:

- *atomic-write barrier*: a half-populated dir from an interrupted write
  has no sentinel, so won't be served.
- *terminal-status flag*: completed meetings never need re-fetching, so
  `_is_cache_fresh` short-circuits when this returns True.

Pre-consolidation versions of this code also maintained a separate
`<cache>/completed/<id>` marker dir. That second source of truth has been
removed; the legacy directory is deleted at startup if found.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

log = logging.getLogger(__name__)

_COMPLETE_SENTINEL = ".complete"


class StatusCache:
    """Reads completion status from the `.complete` sentinel in each
    meeting's detail cache directory.
    """

    def __init__(self, cache_dir: Path | None = None) -> None:
        # cache_dir is the cache *root* (e.g., ~/.cache/fireflies-meetings).
        self._cache_dir = cache_dir or (
            Path(os.environ.get("XDG_CACHE_HOME", "~/.cache")).expanduser()
            / "fireflies-meetings"
        )
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._detail_dir = self._cache_dir / "detail"
        self._detail_dir.mkdir(parents=True, exist_ok=True)
        self._purge_legacy_completed_dir()

    def is_completed(self, meeting_id: str) -> bool:
        return (self._detail_dir / meeting_id / _COMPLETE_SENTINEL).exists()

    def mark_completed(self, meeting_id: str) -> None:
        """No-op. The sentinel is written by `_save_detail_to_disk` after the
        rendered files; marking without writing files would leave nothing to
        serve. Kept on the interface so callers don't have to know the
        invariant — call sites can stay symmetric with the disk write.
        """

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    def _purge_legacy_completed_dir(self) -> None:
        """Drop the pre-consolidation `<cache>/completed/` marker dir if it
        exists. The `.complete` sentinels in each detail dir are now the
        single source of truth.
        """
        legacy = self._cache_dir / "completed"
        if not legacy.is_dir():
            return
        try:
            shutil.rmtree(legacy)
        except OSError:
            log.warning("Could not remove legacy completed dir at %s", legacy, exc_info=True)
            return
        log.info("Removed legacy completed/ marker dir; sentinel-only from now on")
