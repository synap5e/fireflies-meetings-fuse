"""Cache-root helper retained for the mount wiring.

Completion state now lives in the capture/projection model rather than a
separate marker file. This class owns only the cache directory path so existing
startup code and tests can keep passing one object around.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path


class StatusCache:
    def __init__(self, cache_dir: Path | None = None) -> None:
        self._cache_dir = cache_dir or (
            Path(os.environ.get("XDG_CACHE_HOME", "~/.cache")).expanduser()
            / "fireflies-meetings"
        )
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._purge_legacy_completed_dir()

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    def _purge_legacy_completed_dir(self) -> None:
        legacy = self._cache_dir / "completed"
        if legacy.is_dir():
            shutil.rmtree(legacy, ignore_errors=True)
