"""Outcome model for the best-effort Fireflies access-log endpoint."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .models import AccessLogEntry


@dataclass(frozen=True)
class AccessLogsOk:
    """The endpoint succeeded; ``logs`` may legitimately be empty."""

    outcome: Literal["ok"] = "ok"
    logs: tuple[AccessLogEntry, ...] = ()


@dataclass(frozen=True)
class AccessLogsPending:
    """The endpoint has not been attempted for this meeting."""

    outcome: Literal["pending"] = "pending"


@dataclass(frozen=True)
class AccessLogsFailed:
    """The endpoint was attempted but did not return a usable response."""

    outcome: Literal["failed"] = "failed"


type AccessLogsOutcome = AccessLogsOk | AccessLogsPending | AccessLogsFailed

ACCESS_LOGS_PENDING = AccessLogsPending()
ACCESS_LOGS_FAILED = AccessLogsFailed()


def access_logs_ok(logs: list[AccessLogEntry] | tuple[AccessLogEntry, ...]) -> AccessLogsOk:
    """Build a successful outcome while normalizing its rows to a tuple."""
    return AccessLogsOk(logs=tuple(logs))
