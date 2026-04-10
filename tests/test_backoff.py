# pyright: reportPrivateUsage=false
"""Tests for _BackoffState jitter math.

Two regressions to lock in:
- record_rate_limit must NEVER schedule a retry earlier than the
  server-supplied retry_after window. Symmetric jitter could push the
  retry up to 25% earlier and trigger a self-inflicted 429 loop.
- record_failure may use symmetric jitter (the delay is self-chosen).
"""

from __future__ import annotations

import time

from fireflies_meetings.store import _BACKOFF_INITIAL, _BACKOFF_MAX, _BackoffState


def test_record_rate_limit_never_schedules_before_server_window() -> None:
    """1000 trials of a 60s server window: until - now must always be >= 60.0 (positive jitter)."""
    for _ in range(1000):
        state = _BackoffState()
        before = time.monotonic()
        state.record_rate_limit(60.0)
        elapsed = time.monotonic() - before
        wait = state.until - time.monotonic() - elapsed
        # The scheduled retry must be at LEAST the server-provided window away.
        assert state.until - before >= 60.0, (
            f"record_rate_limit scheduled retry {state.until - before:.3f}s after now, "
            f"earlier than server-provided 60.0s window"
        )
        _ = wait  # silence unused


def test_record_rate_limit_jitter_is_bounded_above() -> None:
    """Positive jitter should still be bounded — no more than +25% over the base delay."""
    base = 60.0
    state = _BackoffState()
    overheads: list[float] = []
    for _ in range(1000):
        state = _BackoffState()
        before = time.monotonic()
        state.record_rate_limit(base)
        overheads.append(state.until - before - base)
    assert max(overheads) <= base * 0.25 + 0.05  # +25% +tiny epsilon for monotonic drift
    assert min(overheads) >= 0.0  # never negative


def test_record_rate_limit_uses_initial_when_no_retry_after() -> None:
    state = _BackoffState()
    state.record_rate_limit(None)
    # Should fall back to >= _BACKOFF_INITIAL
    assert state.until - time.monotonic() >= _BACKOFF_INITIAL * 0.5  # generous lower bound


def test_record_failure_jitter_is_symmetric() -> None:
    """record_failure uses self-chosen delays so symmetric jitter is fine."""
    state = _BackoffState()
    state.delay = 100.0  # set a known delay
    state.consecutive_timeouts = 0

    deltas: list[float] = []
    for _ in range(2000):
        state2 = _BackoffState()
        state2.delay = 100.0  # so the doubling lands at 200.0
        before = time.monotonic()
        state2.record_failure()
        # delay should now be 200.0 (doubled), jitter ±25% → [150, 250]
        deltas.append(state2.until - before - 200.0)
    assert min(deltas) < -10.0  # at least some samples land below
    assert max(deltas) > 10.0  # at least some samples land above


def test_record_success_resets_state() -> None:
    state = _BackoffState()
    state.record_failure()
    assert state.delay > 0
    state.record_success()
    assert state.delay == 0.0
    assert state.until == 0.0
    assert state.consecutive_timeouts == 0


def test_backoff_doubles_until_max() -> None:
    state = _BackoffState()
    for _ in range(20):
        state.record_failure()
    assert state.delay == _BACKOFF_MAX


def test_is_backed_off_after_failure() -> None:
    state = _BackoffState()
    assert not state.is_backed_off
    state.record_failure()
    assert state.is_backed_off


def test_is_backed_off_after_fatal() -> None:
    state = _BackoffState()
    state.record_fatal()
    assert state.is_backed_off
    assert state.fatal
