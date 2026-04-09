"""Slugify text for use as filenames."""

from __future__ import annotations

import re
import unicodedata

_MAX_LEN = 60


def slugify(text: str) -> str:
    """Convert text to a URL/filename-safe slug, truncated on word boundary.

    - Normalizes unicode to ASCII
    - Lowercases
    - Replaces non-alphanumeric runs with single hyphens
    - Truncates to ~60 chars on word boundary
    - Strips leading/trailing hyphens
    """
    if not text or not text.strip():
        return ""

    # Normalize unicode → ASCII approximation
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")

    # Lowercase
    text = text.lower()

    # Replace non-alphanumeric with hyphens, collapse runs
    text = re.sub(r"[^a-z0-9]+", "-", text)

    # Strip leading/trailing hyphens
    text = text.strip("-")

    if not text:
        return ""

    # Truncate on word boundary
    if len(text) <= _MAX_LEN:
        return text

    truncated = text[:_MAX_LEN]
    # Find last hyphen to break on word boundary
    last_hyphen = truncated.rfind("-")
    if last_hyphen > 20:  # Only break on word boundary if reasonable
        truncated = truncated[:last_hyphen]

    return truncated
