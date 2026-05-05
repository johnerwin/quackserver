"""Tests for HealthSnapshot and RuntimeState."""

import pytest

from quackserver.config import WRITE_QUEUE_WARNING_PCT
from quackserver.core.health import HealthSnapshot, RuntimeState


def _snap(**kw) -> HealthSnapshot:
    defaults = dict(
        queue_depth=0,
        queue_max=1000,
        worker_running=True,
        replay_pending=0,
        malformed_log_entries=0,
        state=RuntimeState.RUNNING,
        state_since="2026-05-05T00:00:00+00:00",
    )
    defaults.update(kw)
    return HealthSnapshot(**defaults)


class TestHealthSnapshot:
    def test_is_frozen(self):
        snap = _snap()
        with pytest.raises((AttributeError, TypeError)):
            snap.queue_depth = 99  # type: ignore[misc]

    def test_uses_slots(self):
        assert hasattr(HealthSnapshot, "__slots__")

    def test_healthy_baseline(self):
        assert _snap().is_healthy is True

    def test_unhealthy_when_worker_not_running(self):
        assert _snap(worker_running=False).is_healthy is False

    def test_unhealthy_when_replay_pending(self):
        assert _snap(replay_pending=5).is_healthy is False

    def test_unhealthy_when_queue_full(self):
        assert _snap(queue_depth=1000, queue_max=1000).is_healthy is False

    def test_healthy_when_queue_at_warning_threshold(self):
        depth = int(1000 * WRITE_QUEUE_WARNING_PCT / 100)
        assert _snap(queue_depth=depth).is_healthy is True

    def test_unhealthy_when_state_is_recovering(self):
        assert _snap(state=RuntimeState.RECOVERING).is_healthy is False

    def test_unhealthy_when_state_is_starting(self):
        assert _snap(state=RuntimeState.STARTING).is_healthy is False

    def test_unhealthy_when_state_is_stopping(self):
        assert _snap(state=RuntimeState.STOPPING).is_healthy is False

    def test_unhealthy_when_state_is_failed(self):
        assert _snap(state=RuntimeState.FAILED).is_healthy is False

    def test_queue_pct_zero_when_empty(self):
        assert _snap(queue_depth=0).queue_pct == 0.0

    def test_queue_pct_100_when_full(self):
        assert _snap(queue_depth=1000, queue_max=1000).queue_pct == 100.0

    def test_queue_pct_rounded_to_one_decimal(self):
        snap = _snap(queue_depth=333, queue_max=1000)
        assert snap.queue_pct == 33.3

    def test_queue_pct_zero_max_returns_zero(self):
        snap = _snap(queue_depth=0, queue_max=0)
        assert snap.queue_pct == 0.0

    def test_is_queue_warning_false_below_threshold(self):
        depth = int(1000 * WRITE_QUEUE_WARNING_PCT / 100) - 1
        assert _snap(queue_depth=depth).is_queue_warning is False

    def test_is_queue_warning_true_at_threshold(self):
        depth = int(1000 * WRITE_QUEUE_WARNING_PCT / 100)
        assert _snap(queue_depth=depth).is_queue_warning is True

    def test_is_queue_warning_true_above_threshold(self):
        assert _snap(queue_depth=900, queue_max=1000).is_queue_warning is True

    def test_malformed_entries_do_not_affect_health(self):
        # Corruption is observable via the snapshot but does not bring the
        # system down — operators must investigate separately.
        assert _snap(malformed_log_entries=3).is_healthy is True

    def test_state_field_accessible(self):
        snap = _snap(state=RuntimeState.RUNNING)
        assert snap.state == RuntimeState.RUNNING

    def test_state_since_field_accessible(self):
        snap = _snap(state_since="2026-05-05T12:00:00+00:00")
        assert snap.state_since == "2026-05-05T12:00:00+00:00"


class TestRuntimeState:
    def test_all_states_defined(self):
        names = {s.name for s in RuntimeState}
        assert names == {"STOPPED", "STARTING", "RECOVERING", "RUNNING", "STOPPING", "FAILED"}

    def test_states_are_distinct(self):
        states = list(RuntimeState)
        assert len(states) == len(set(states))

    def test_failed_is_not_healthy(self):
        # Belt-and-suspenders: FAILED must never appear healthy regardless of
        # other fields being in their "good" state.
        snap = _snap(
            state=RuntimeState.FAILED,
            worker_running=True,
            replay_pending=0,
            queue_depth=0,
        )
        assert snap.is_healthy is False
