"""
quackserver serve — start the HTTP server.

Usage:
    python -m quackserver serve
    python -m quackserver serve --db-path ./data/quack.duckdb --log-path ./data/quack.jsonl
    python -m quackserver serve --host 0.0.0.0 --port 8000

Default paths place all state files next to each other in ./data/:
    ./data/quack.duckdb      - DuckDB projection (derived, disposable)
    ./data/quack.jsonl       - Append log (canonical truth)
    ./data/dedup.db          - Idempotency cache (SQLite)
    ./data/checkpoint.db     - Replay checkpoint (SQLite)

The dedup and checkpoint paths are derived from the log-path directory
and cannot be set independently (they must stay in the same directory as
the log so they can be found after crash recovery).
"""

import argparse
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="quackserver serve",
        description="Start the quackserver HTTP API.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/quack.duckdb"),
        metavar="PATH",
        help="Path to the DuckDB file (default: data/quack.duckdb).",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=Path("data/quack.jsonl"),
        metavar="PATH",
        help="Path to the JSONL append log (default: data/quack.jsonl).",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        metavar="HOST",
        help="Bind host (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        metavar="PORT",
        help="Bind port (default: 8000).",
    )
    args = parser.parse_args(argv)

    data_dir = args.log_path.parent
    data_dir.mkdir(parents=True, exist_ok=True)

    dedup_path = data_dir / "dedup.db"
    checkpoint_path = data_dir / "checkpoint.db"

    import uvicorn

    from quackserver.core.runtime import Runtime
    from quackserver.http.app import create_app
    from quackserver.storage.duckdb_impl import DuckDBProjectionStore

    store = DuckDBProjectionStore(args.db_path)
    runtime = Runtime(
        log_path=args.log_path,
        dedup_path=dedup_path,
        checkpoint_path=checkpoint_path,
        projection=store,
        read_store=store,
    )
    app = create_app(runtime)

    print(f"quackserver starting")
    print(f"  db   : {args.db_path.resolve()}")
    print(f"  log  : {args.log_path.resolve()}")
    print(f"  bind : http://{args.host}:{args.port}")

    uvicorn.run(app, host=args.host, port=args.port)
