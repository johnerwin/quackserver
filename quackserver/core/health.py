"""
HealthSnapshot — a point-in-time, deterministic view of runtime state.

Not ad hoc dicts. Not framework objects. A frozen value that:
  - is fully serialisable without conversion gymnastics
  - can be constructed in tests without running a server
  - is the single source for both /health HTTP responses and structured logs
  - carries enough signal for operators to triage load and replay issues

The HTTP layer converts this to JSON. The Runtime produces it. Neither
layer needs to know how the other works.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from quackserver.config import WRITE_QUEUE_WARNING_PCT


class RuntimeState(Enum):
    """Explicit lifecycle states for the Runtime.

    Legal transitions:
        STOPPED   → STARTING
        STARTING  → RECOVERING | FAILED
        RECOVERING → RUNNING   | FAILED
        RUNNING   → STOPPING   | FAILED
        STOPPING  → STOPPED    | FAILED
        FAILED    → STOPPED    (cleanup only; requires full restart to reach RUNNING)

    FAILED is sticky: the system cannot transition from FAILED back to any
    operational state without going through STOPPED → STARTING first. This
    is intentional — it forces operator visibility of integrity failures
    rather than allowing silent auto-recovery that masks root causes.
    """

    STOPPED = auto()
    STARTING = auto()
    RECOVERING = auto()
    RUNNING = auto()
    STOPPING = auto()
    FAILED = auto()


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    """Immutable runtime health state at a single instant.

    Fields:
        queue_depth:           Current number of entries in the write queue.
        queue_max:             Configured maximum queue size (MAX_WRITE_QUEUE_SIZE).
        worker_running:        True iff the write worker task is alive.
        replay_pending:        Entries still pending in the log at startup; 0
                               after recovery completes (spec §7.3).
        malformed_log_entries: Cumulative count of JSONL lines that could not
                               be parsed during log reads. Any value > 0 means
                               the log has corruption that needs investigation.
        state:                 Current lifecycle state of the Runtime.
        state_since:           ISO 8601 UTC timestamp of the last state transition.
    """

    queue_depth: int
    queue_max: int
    worker_running: bool
    replay_pending: int
    malformed_log_entries: int
    state: RuntimeState
    state_since: str  # ISO 8601 UTC

    @property
    def queue_pct(self) -> float:
        """Queue occupancy as a percentage (0.0 – 100.0)."""
        if self.queue_max <= 0:
            return 0.0
        return round(self.queue_depth / self.queue_max * 100, 1)

    @property
    def is_queue_warning(self) -> bool:
        """True when queue occupancy has crossed the warning threshold.

        At this level operators should investigate write throughput.
        The system still accepts writes; rejection happens at 100%.
        """
        return self.queue_pct >= WRITE_QUEUE_WARNING_PCT

    @property
    def is_healthy(self) -> bool:
        """True iff the runtime is fully operational and accepting writes.

        Conditions that make this False:
          - Runtime is not in RUNNING state (starting, recovering, stopping, failed)
          - Worker task is not running
          - Recovery has pending entries (startup incomplete)
          - Queue is at 100% capacity (writes being rejected)

        Queue warning (80%) does not make the system unhealthy — it is a
        pressure signal, not a failure. The /health endpoint returns 200
        for warning state so that load balancers do not route away
        prematurely.
        """
        return (
            self.state == RuntimeState.RUNNING
            and self.worker_running
            and self.replay_pending == 0
            and self.queue_depth < self.queue_max
        )
