"""Shared test fixtures and doubles."""

from quackserver.core.commands import Command
from quackserver.core.projection import ProjectionStore
from quackserver.storage.interface import Result


class MockProjection(ProjectionStore):
    """Records calls and returns configurable results.

    Tracks calls as (command_name, request_id) tuples — same shape as the
    old MockStorage so existing assertions in test_runtime.py still work.
    """

    def __init__(self, result: Result | None = None) -> None:
        self._result = result or Result(success=True, status_code=200)
        self.calls: list[tuple[str, str]] = []  # (command_name, request_id)

    async def apply(self, command: Command) -> Result:
        self.calls.append((command.command, command.request_id))
        return Result(
            success=self._result.success,
            status_code=self._result.status_code,
            request_id=command.request_id,
            data={"status": "accepted"} if self._result.success else None,
        )

    async def health(self) -> Result:
        return Result(success=True, status_code=200)

    async def close(self) -> None:
        pass
