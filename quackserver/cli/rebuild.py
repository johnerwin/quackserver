"""
quackserver rebuild — rebuild the DuckDB projection from the append log.

Usage:
    python -m quackserver rebuild --log-path ./quack.jsonl --db-path ./quack.duckdb
    python -m quackserver rebuild --log-path ./quack.jsonl --db-path ./quack.duckdb --overwrite

Because DuckDB is disposable derived state (spec §6.2), a full rebuild is
the correct compaction strategy:

    1. Delete the existing DuckDB file (and its WAL if present).
    2. Replay every entry in the append log through DuckDBProjectionStore.
    3. The result is a compact file containing exactly the live rows — no
       fragmentation, no dead space from overwritten UPSERT targets.

Duplicate request_ids in the log are deduplicated before replay, matching
the same semantics used during normal crash recovery.
"""

import argparse
import asyncio
import time
from pathlib import Path

from quackserver.core.commands import Command
from quackserver.core.log import AppendLogReader
from quackserver.storage.duckdb_impl import DuckDBProjectionStore


async def _run(log_path: Path, db_path: Path) -> dict:
    reader = AppendLogReader(log_path)
    store = DuckDBProjectionStore(db_path)
    await store.initialize()

    t0 = time.monotonic()
    counts: dict[str, int] = {
        "total": 0,
        "create_user": 0,
        "append_event": 0,
        "unknown_command": 0,
        "errors": 0,
    }
    seen: set[str] = set()

    for cmd in reader.entries():
        if cmd.request_id in seen:
            continue
        seen.add(cmd.request_id)

        result = await store.apply(cmd)
        counts["total"] += 1
        if result.success:
            if cmd.command == "create_user":
                counts["create_user"] += 1
            elif cmd.command == "append_event":
                counts["append_event"] += 1
            else:
                counts["unknown_command"] += 1
        else:
            counts["errors"] += 1

    rebuilt_at = Command.now_utc()
    await store.record_rebuild(
        log_path=str(log_path),
        entry_count=counts["total"],
        rebuilt_at=rebuilt_at,
    )
    await store.close()
    return {
        **counts,
        "malformed_log_lines": reader.malformed_count,
        "duration_ms": int((time.monotonic() - t0) * 1000),
    }


def _delete_db(db_path: Path) -> None:
    db_path.unlink()
    wal = Path(str(db_path) + ".wal")
    if wal.exists():
        wal.unlink()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="quackserver rebuild",
        description="Rebuild the DuckDB projection from the append log.",
    )
    parser.add_argument(
        "--log-path",
        required=True,
        type=Path,
        metavar="PATH",
        help="Path to the JSONL append log (source of truth).",
    )
    parser.add_argument(
        "--db-path",
        required=True,
        type=Path,
        metavar="PATH",
        help="Destination path for the rebuilt DuckDB file.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace db-path if it already exists.",
    )
    args = parser.parse_args(argv)

    if not args.log_path.exists():
        parser.error(f"append log not found: {args.log_path}")

    if args.db_path.exists():
        if not args.overwrite:
            parser.error(
                f"{args.db_path} already exists. Pass --overwrite to replace it."
            )
        _delete_db(args.db_path)

    print(f"Rebuilding {args.db_path}")
    print(f"       from {args.log_path} ...")

    stats = asyncio.run(_run(args.log_path, args.db_path))

    print(f"  entries replayed : {stats['total']}")
    print(f"  users            : {stats['create_user']}")
    print(f"  events           : {stats['append_event']}")
    if stats["unknown_command"]:
        print(f"  unknown commands : {stats['unknown_command']}")
    if stats["errors"]:
        print(f"  errors           : {stats['errors']}")
    if stats["malformed_log_lines"]:
        print(f"  malformed lines  : {stats['malformed_log_lines']}")
    print(f"  duration         : {stats['duration_ms']}ms")
    print("Done.")
