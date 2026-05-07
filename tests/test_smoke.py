"""
Phase 1 smoke tests — structural correctness only.

These tests verify that the project skeleton is wired correctly before any
implementation begins. They are not testing behaviour; they are testing shape.

All tests must pass before Phase 2 work begins.
"""

import ast
import importlib
import inspect
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
PACKAGE_ROOT = PROJECT_ROOT / "quackserver"


def _source_imports(path: Path) -> list[str]:
    """Return all top-level module names imported by a source file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.append(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported.append(node.module.split(".")[0])
    return imported


# ---------------------------------------------------------------------------
# Directory structure
# ---------------------------------------------------------------------------


class TestDirectoryStructure:
    def test_package_exists(self):
        assert (PACKAGE_ROOT / "__init__.py").is_file()

    def test_storage_package_exists(self):
        assert (PACKAGE_ROOT / "storage" / "__init__.py").is_file()

    def test_config_exists(self):
        assert (PACKAGE_ROOT / "config.py").is_file()

    def test_storage_interface_exists(self):
        assert (PACKAGE_ROOT / "storage" / "interface.py").is_file()

    def test_storage_duckdb_impl_exists(self):
        assert (PACKAGE_ROOT / "storage" / "duckdb_impl.py").is_file()

    def test_tests_directory_exists(self):
        assert (PROJECT_ROOT / "tests").is_dir()


# ---------------------------------------------------------------------------
# config.py — every named constant from the spec must be present and typed
# ---------------------------------------------------------------------------


class TestConfig:
    @pytest.fixture(autouse=True)
    def _import(self):
        import quackserver.config as cfg

        self.cfg = cfg

    # Presence checks — one per constant in Section 3 and Section 9.2

    def test_max_write_queue_size(self):
        assert isinstance(self.cfg.MAX_WRITE_QUEUE_SIZE, int)
        assert self.cfg.MAX_WRITE_QUEUE_SIZE == 1_000

    def test_max_write_time_ms(self):
        assert isinstance(self.cfg.MAX_WRITE_TIME_MS, int)
        assert self.cfg.MAX_WRITE_TIME_MS == 500

    def test_max_write_retries(self):
        assert isinstance(self.cfg.MAX_WRITE_RETRIES, int)
        assert self.cfg.MAX_WRITE_RETRIES == 3

    def test_write_queue_warning_pct(self):
        assert isinstance(self.cfg.WRITE_QUEUE_WARNING_PCT, int)
        assert self.cfg.WRITE_QUEUE_WARNING_PCT == 80

    def test_max_read_query_time_ms(self):
        assert isinstance(self.cfg.MAX_READ_QUERY_TIME_MS, int)
        assert self.cfg.MAX_READ_QUERY_TIME_MS == 10_000

    def test_max_concurrent_reads(self):
        assert isinstance(self.cfg.MAX_CONCURRENT_READS, int)
        assert self.cfg.MAX_CONCURRENT_READS == 10

    def test_read_pool_size(self):
        assert isinstance(self.cfg.READ_POOL_SIZE, int)
        assert self.cfg.READ_POOL_SIZE == 3

    def test_dashboard_query_timeout_ms(self):
        assert isinstance(self.cfg.DASHBOARD_QUERY_TIMEOUT_MS, int)
        assert self.cfg.DASHBOARD_QUERY_TIMEOUT_MS == 3_000

    def test_dashboard_max_rows(self):
        assert isinstance(self.cfg.DASHBOARD_MAX_ROWS, int)
        assert self.cfg.DASHBOARD_MAX_ROWS == 10_000

    def test_report_query_timeout_ms(self):
        assert isinstance(self.cfg.REPORT_QUERY_TIMEOUT_MS, int)
        assert self.cfg.REPORT_QUERY_TIMEOUT_MS == 10_000

    def test_report_max_rows(self):
        assert isinstance(self.cfg.REPORT_MAX_ROWS, int)
        assert self.cfg.REPORT_MAX_ROWS == 100_000

    def test_event_query_timeout_ms(self):
        assert isinstance(self.cfg.EVENT_QUERY_TIMEOUT_MS, int)
        assert self.cfg.EVENT_QUERY_TIMEOUT_MS == 5_000

    def test_event_max_rows(self):
        assert isinstance(self.cfg.EVENT_MAX_ROWS, int)
        assert self.cfg.EVENT_MAX_ROWS == 50_000

    def test_health_query_timeout_ms(self):
        assert isinstance(self.cfg.HEALTH_QUERY_TIMEOUT_MS, int)
        assert self.cfg.HEALTH_QUERY_TIMEOUT_MS == 500

    def test_health_max_rows(self):
        assert isinstance(self.cfg.HEALTH_MAX_ROWS, int)
        assert self.cfg.HEALTH_MAX_ROWS == 100

    def test_max_connections(self):
        assert isinstance(self.cfg.MAX_CONNECTIONS, int)
        assert self.cfg.MAX_CONNECTIONS == 50

    def test_max_payload_size_mb(self):
        assert isinstance(self.cfg.MAX_PAYLOAD_SIZE_MB, int)
        assert self.cfg.MAX_PAYLOAD_SIZE_MB == 10

    def test_max_result_rows(self):
        assert isinstance(self.cfg.MAX_RESULT_ROWS, int)
        assert self.cfg.MAX_RESULT_ROWS == 100_000

    def test_idempotency_key_ttl_hours(self):
        assert isinstance(self.cfg.IDEMPOTENCY_KEY_TTL_HOURS, int)
        assert self.cfg.IDEMPOTENCY_KEY_TTL_HOURS == 24

    # Sanity relationships

    def test_warning_pct_below_100(self):
        assert 0 < self.cfg.WRITE_QUEUE_WARNING_PCT < 100

    def test_dashboard_timeout_less_than_report_timeout(self):
        # Dashboard should always be faster than full report generation.
        assert self.cfg.DASHBOARD_QUERY_TIMEOUT_MS < self.cfg.REPORT_QUERY_TIMEOUT_MS

    def test_health_timeout_is_fastest(self):
        assert self.cfg.HEALTH_QUERY_TIMEOUT_MS <= self.cfg.DASHBOARD_QUERY_TIMEOUT_MS

    def test_result_rows_gte_per_endpoint_max(self):
        # Global ceiling must not be lower than any per-endpoint limit.
        assert self.cfg.MAX_RESULT_ROWS >= self.cfg.DASHBOARD_MAX_ROWS
        assert self.cfg.MAX_RESULT_ROWS >= self.cfg.REPORT_MAX_ROWS
        assert self.cfg.MAX_RESULT_ROWS >= self.cfg.EVENT_MAX_ROWS

    # Enforcement of the non-negotiable rule: config.py must never import duckdb.

    def test_config_does_not_import_duckdb(self):
        imports = _source_imports(PACKAGE_ROOT / "config.py")
        assert "duckdb" not in imports, (
            "config.py must not import duckdb — only storage/duckdb_impl.py may do so"
        )


# ---------------------------------------------------------------------------
# storage/interface.py — shape and contract
# ---------------------------------------------------------------------------


class TestStorageInterface:
    @pytest.fixture(autouse=True)
    def _import(self):
        from quackserver.storage.interface import Result, StorageInterface

        self.StorageInterface = StorageInterface
        self.Result = Result

    def test_storage_interface_is_abstract(self):
        with pytest.raises(TypeError):
            self.StorageInterface()  # type: ignore[abstract]

    def test_result_is_dataclass(self):
        r = self.Result(success=True, status_code=200, data={"rows": 0}, request_id="abc")
        assert r.success is True
        assert r.status_code == 200
        assert r.data == {"rows": 0}
        assert r.request_id == "abc"
        assert r.error is None

    def test_result_failure_case(self):
        r = self.Result(success=False, status_code=503, error="queue full")
        assert r.success is False
        assert r.status_code == 503
        assert r.error == "queue full"

    def test_result_uses_slots(self):
        assert hasattr(self.Result, "__slots__"), "Result must use slots=True"

    def test_result_status_code_is_required(self):
        with pytest.raises(TypeError):
            self.Result(success=True)  # missing status_code

    def test_result_duration_ms_is_int_or_none(self):
        r = self.Result(success=True, status_code=200, duration_ms=42)
        assert isinstance(r.duration_ms, int)
        r2 = self.Result(success=True, status_code=200)
        assert r2.duration_ms is None

    # Verify every required abstract method is declared.

    @pytest.mark.parametrize(
        "method_name",
        [
            "create_user",
            "append_event",
            "get_dashboard_metrics",
            "query_events",
            "check_health",
            "bulk_import",
            "close",
        ],
    )
    def test_abstract_method_exists(self, method_name: str):
        assert hasattr(self.StorageInterface, method_name), (
            f"StorageInterface is missing method: {method_name}"
        )

    @pytest.mark.parametrize(
        "method_name",
        [
            "create_user",
            "append_event",
            "get_dashboard_metrics",
            "query_events",
            "check_health",
            "bulk_import",
            "close",
        ],
    )
    def test_method_is_abstract(self, method_name: str):
        method = getattr(self.StorageInterface, method_name)
        assert getattr(method, "__isabstractmethod__", False), (
            f"{method_name} must be declared @abstractmethod"
        )

    @pytest.mark.parametrize(
        "method_name",
        ["create_user", "append_event", "get_dashboard_metrics", "query_events",
         "check_health", "bulk_import"],
    )
    def test_method_is_coroutine(self, method_name: str):
        method = getattr(self.StorageInterface, method_name)
        assert inspect.iscoroutinefunction(method), (
            f"{method_name} must be declared async"
        )

    def test_concrete_subclass_must_implement_all(self):
        """A subclass that skips even one method cannot be instantiated."""

        class Incomplete(self.StorageInterface):
            async def create_user(self, request_id, user_id, payload):
                return self.Result(success=True)

            # intentionally missing all others

        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]

    def test_complete_subclass_is_instantiable(self):
        """A subclass that implements every method can be instantiated."""
        Result = self.Result

        class Complete(self.StorageInterface):
            async def create_user(self, request_id, user_id, payload):
                return Result(success=True, status_code=200)

            async def append_event(self, request_id, event_type, payload):
                return Result(success=True, status_code=200)

            async def get_dashboard_metrics(self, filters):
                return Result(success=True, status_code=200)

            async def query_events(self, filters, limit):
                return Result(success=True, status_code=200)

            async def check_health(self):
                return Result(success=True, status_code=200)

            async def bulk_import(self, source_path):
                return Result(success=True, status_code=200)

            async def close(self):
                pass

        instance = Complete()
        assert isinstance(instance, self.StorageInterface)

    # Enforcement: interface.py must not import duckdb.

    def test_interface_does_not_import_duckdb(self):
        imports = _source_imports(PACKAGE_ROOT / "storage" / "interface.py")
        assert "duckdb" not in imports, (
            "storage/interface.py must not import duckdb — only duckdb_impl.py may do so"
        )


# ---------------------------------------------------------------------------
# duckdb_impl.py — isolation guard
# ---------------------------------------------------------------------------


class TestDuckdbImplIsolation:
    def test_duckdb_impl_is_the_only_duckdb_importer(self):
        """Walk every .py file in the package and assert duckdb is only in duckdb_impl.py.

        CLI tools in quackserver/cli/ are standalone utilities that necessarily
        import duckdb directly — they are not part of the server's layered
        architecture and are explicitly excluded from this constraint.
        """
        cli_dir = PACKAGE_ROOT / "cli"
        violations = []
        for py_file in PACKAGE_ROOT.rglob("*.py"):
            if py_file.name == "duckdb_impl.py":
                continue
            if py_file.is_relative_to(cli_dir):
                continue  # CLI tools are standalone; duckdb is allowed there
            imports = _source_imports(py_file)
            if "duckdb" in imports:
                violations.append(str(py_file.relative_to(PROJECT_ROOT)))
        assert violations == [], (
            f"duckdb imported outside duckdb_impl.py (and cli/): {violations}"
        )


# ---------------------------------------------------------------------------
# Invariant tests — philosophy encoded as tripwires (spec §12)
#
# These tests are not checking structure. They are checking that architectural
# constraints from the spec cannot be accidentally eroded as Phase 2+ adds code.
# ---------------------------------------------------------------------------


class TestInvariants:
    @pytest.fixture(autouse=True)
    def _imports(self):
        import quackserver.config as cfg
        from quackserver.storage.interface import Result, StorageInterface

        self.cfg = cfg
        self.StorageInterface = StorageInterface
        self.Result = Result

    # --- Config invariants ---------------------------------------------------

    @pytest.mark.parametrize(
        "name",
        [
            "MAX_WRITE_QUEUE_SIZE",
            "MAX_WRITE_TIME_MS",
            "MAX_WRITE_RETRIES",
            "MAX_READ_QUERY_TIME_MS",
            "MAX_CONCURRENT_READS",
            "READ_POOL_SIZE",
            "MAX_CONNECTIONS",
            "MAX_PAYLOAD_SIZE_MB",
            "MAX_RESULT_ROWS",
            "IDEMPOTENCY_KEY_TTL_HOURS",
            "DASHBOARD_QUERY_TIMEOUT_MS",
            "DASHBOARD_MAX_ROWS",
            "REPORT_QUERY_TIMEOUT_MS",
            "REPORT_MAX_ROWS",
            "EVENT_QUERY_TIMEOUT_MS",
            "EVENT_MAX_ROWS",
            "HEALTH_QUERY_TIMEOUT_MS",
            "HEALTH_MAX_ROWS",
        ],
    )
    def test_all_limit_constants_are_positive(self, name: str):
        """Every limit must be > 0. Zero would disable the constraint entirely."""
        value = getattr(self.cfg, name)
        assert value > 0, f"{name} must be positive, got {value}"

    def test_write_timeout_shorter_than_read_timeout(self):
        """Write transactions must be shorter than reads.
        A slow write holds the sole write connection; reads use separate connections
        but share the event loop. Write timeout must be the tighter constraint."""
        assert self.cfg.MAX_WRITE_TIME_MS < self.cfg.MAX_READ_QUERY_TIME_MS

    def test_warning_fires_before_hard_limit(self):
        """Warning threshold must be below 100% so operators see signal before rejection."""
        assert self.cfg.WRITE_QUEUE_WARNING_PCT < 100

    def test_read_pool_smaller_than_concurrency_cap(self):
        """Connection pool size should not exceed the semaphore slot count.
        Extra connections beyond the semaphore cap would sit idle permanently."""
        assert self.cfg.READ_POOL_SIZE <= self.cfg.MAX_CONCURRENT_READS

    # --- Interface invariants ------------------------------------------------

    @pytest.mark.parametrize("method_name", ["create_user", "append_event"])
    def test_write_methods_have_request_id_as_first_param(self, method_name: str):
        """Every write method must declare request_id as the first parameter after self.
        The spec treats a missing request_id as a hard 400 (spec §8.1). If the
        interface does not enforce its presence in the signature, callers will
        eventually skip it."""
        method = getattr(self.StorageInterface, method_name)
        sig = inspect.signature(method)
        params = [p for p in sig.parameters if p != "self"]
        assert params[0] == "request_id", (
            f"{method_name}: first parameter must be 'request_id', got '{params[0]}'"
        )

    def test_no_method_accepts_raw_sql_parameter(self):
        """No StorageInterface method may accept a parameter named 'sql',
        'query_str', or 'raw_sql'. Raw SQL must never cross the interface boundary
        (spec §2.4, §12.1)."""
        forbidden_params = {"sql", "query_str", "raw_sql"}
        for name, method in inspect.getmembers(self.StorageInterface, predicate=callable):
            if name.startswith("_"):
                continue
            sig = inspect.signature(method)
            overlap = forbidden_params & set(sig.parameters)
            assert not overlap, (
                f"{name}() has forbidden SQL parameter(s): {overlap}"
            )

    def test_no_raw_sql_method_names(self):
        """No method should be named in a way that exposes raw SQL execution.
        Once a 'query' or 'execute' method exists on the interface, the
        command-oriented constraint is effectively gone (spec §2.4, §12.2)."""
        forbidden_names = {"execute", "execute_sql", "execute_query", "run_sql", "query"}
        method_names = {
            name
            for name, _ in inspect.getmembers(self.StorageInterface, predicate=callable)
            if not name.startswith("_")
        }
        overlap = method_names & forbidden_names
        assert not overlap, (
            f"StorageInterface exposes raw-SQL method name(s): {overlap}"
        )

    def test_no_mutable_default_args_in_storage_and_core(self):
        """Mutable default arguments (list, dict, set) in method signatures are a
        classic Python footgun — the default object is shared across all calls.
        In an async context this becomes a silent data corruption bug."""
        violations = []
        for subdir in ("storage", "core"):
            for py_file in (PACKAGE_ROOT / subdir).rglob("*.py"):
                tree = ast.parse(py_file.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    for default in node.args.defaults + node.args.kw_defaults:
                        if default is None:
                            continue
                        if isinstance(default, (ast.List, ast.Dict, ast.Set)):
                            violations.append(
                                f"{py_file.name}:{node.lineno} {node.name}() has mutable default"
                            )
        assert violations == [], "\n".join(violations)

    def test_result_status_codes_align_with_spec(self):
        """Spot-check that the failure semantics table (spec §4) can be expressed
        with the Result type. Each documented failure mode must have a valid HTTP
        status code constant to map to."""
        spec_status_codes = {200, 400, 404, 413, 503, 504}
        for code in spec_status_codes:
            r = self.Result(success=(code < 400), status_code=code)
            assert r.status_code == code
