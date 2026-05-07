"""
duckdb-inspect - quick read of any DuckDB file.

Usage:
    duckdb-inspect path/to/database.duckdb
    duckdb-inspect path/to/database.duckdb --top 20
    duckdb-inspect path/to/database.duckdb --json

Output sections (human-readable):
  • File info and db_build_metadata (if present)
  • Table inventory: row counts and estimated sizes
  • Top N tables by estimated size
  • Empty tables
  • View inventory and which base tables each view references
  • Duplicate/alias candidates (same schema + same row count)
  • Settings snapshot (key entries only; use --settings for all)

Exit codes:
  0  success
  1  file not found or cannot open
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

from quackserver.cli._duckdb_helpers import (
    base_tables,
    build_metadata,
    col_signatures,
    dup_candidates,
    estimated_sizes,
    fmt_size,
    row_count,
    settings_snapshot,
    topo_sort_views,
    view_definitions,
    view_deps,
    view_names,
)

_SETTINGS_INTEREST = {
    "memory_limit",
    "threads",
    "max_memory",
    "temp_directory",
    "checkpoint_threshold",
    "wal_autocheckpoint",
}


def _probe(conn, db_path: Path, top: int) -> dict:
    tables = base_tables(conn)
    views = view_names(conn)
    sigs = col_signatures(conn)
    vdefs = view_definitions(conn)
    est = estimated_sizes(conn)

    counts: dict[str, int] = {}
    for t in tables:
        counts[t] = row_count(conn, t)

    sorted_by_size = sorted(
        [(t, est.get(t, 0), counts.get(t, 0)) for t in tables],
        key=lambda x: x[1],
        reverse=True,
    )
    empty = [t for t, n in counts.items() if n == 0]
    dups = dup_candidates(tables, sigs, counts)
    vdep = view_deps(vdefs, tables)
    meta = build_metadata(conn)
    snap = settings_snapshot(conn)

    file_size = db_path.stat().st_size

    return {
        "db_path": str(db_path.resolve()),
        "file_size": file_size,
        "table_count": len(tables),
        "view_count": len(views),
        "total_rows": sum(counts.values()),
        "total_estimated_bytes": sum(est.get(t, 0) for t in tables),
        "tables": [
            {
                "name": t,
                "rows": counts.get(t, 0),
                "estimated_bytes": est.get(t, 0),
            }
            for t in tables
        ],
        "top": sorted_by_size[:top],
        "empty": empty,
        "views": [
            {"name": v, "references": vdep.get(v, [])}
            for v in topo_sort_views(vdefs)
        ],
        "dup_candidates": dups,
        "build_metadata": meta,
        "settings": snap,
    }


def _print_report(data: dict, top: int, all_settings: bool) -> None:
    path = data["db_path"]
    file_size = fmt_size(data["file_size"])
    total_est = fmt_size(data["total_estimated_bytes"])

    print(f"\nDatabase : {path}")
    print(f"On disk  : {file_size}")

    meta = data.get("build_metadata")
    if meta:
        compacted_at = meta.get("compacted_at", "")
        fingerprint = meta.get("schema_fingerprint", "")
        rebuilt_at = meta.get("rebuilt_at", "")
        if compacted_at:
            src = meta.get("source_db_path", "")
            print(f"Compacted: {compacted_at}  fingerprint={fingerprint}  (from {src})")
        if rebuilt_at:
            log = meta.get("rebuilt_from_log", "")
            entries = meta.get("replay_entry_count", "?")
            print(f"Rebuilt  : {rebuilt_at}  ({entries} log entries from {log})")

    print(
        f"\nTables   : {data['table_count']}   "
        f"Views : {data['view_count']}   "
        f"Total rows : {data['total_rows']:,}   "
        f"Estimated live : {total_est}"
    )

    # Top N by estimated size
    print(f"\n{'Table':<52}  {'Est size':>9}  {'Rows':>14}")
    print("-" * 80)
    for tname, est_bytes, n in data["top"]:
        est_str = fmt_size(est_bytes) if est_bytes else "-"
        print(f"  {tname:<50}  {est_str:>9}  {n:>14,}")

    remaining = data["table_count"] - len(data["top"])
    if remaining > 0:
        other_rows = data["total_rows"] - sum(r for _, _, r in data["top"])
        print(f"  … {remaining} more tables, {other_rows:,} rows")

    # Empty tables
    empty = data["empty"]
    if empty:
        print(f"\nEmpty tables ({len(empty)}): {', '.join(empty)}")
    else:
        print(f"\nEmpty tables: none")

    # Views
    views = data["views"]
    if views:
        print(f"\nViews ({len(views)}):")
        for v in views:
            refs = v["references"]
            ref_str = ", ".join(refs) if refs else "-"
            print(f"  {v['name']:<40}  -> {ref_str}")
    else:
        print("\nViews: none")

    # Duplicate candidates
    dups = data["dup_candidates"]
    if dups:
        print(f"\nWARN: Duplicate/alias candidates ({len(dups)} group(s)):")
        for group in dups:
            n = next(
                (t["rows"] for t in data["tables"] if t["name"] == group[0]), "?"
            )
            print(f"  {group}  - {n:,} rows, identical schema")
    else:
        print("\nDuplicate/alias candidates: none")

    # Settings
    snap = data["settings"]
    interesting = {k: v for k, v in snap.items() if k in _SETTINGS_INTEREST}
    if all_settings:
        interesting = snap
    if interesting:
        print(f"\nSettings ({'all' if all_settings else 'key subset'}):")
        for k, v in sorted(interesting.items()):
            print(f"  {k}: {v}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="duckdb-inspect",
        description="Quick inspection of any DuckDB database file.",
    )
    parser.add_argument(
        "db_path",
        type=Path,
        metavar="PATH",
        help="Path to the DuckDB file to inspect.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        metavar="N",
        help="Number of largest tables to show (default: 10).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Machine-readable JSON output.",
    )
    parser.add_argument(
        "--settings",
        action="store_true",
        help="Include full settings snapshot (default: key entries only).",
    )
    args = parser.parse_args(argv)

    if not args.db_path.exists():
        print(f"Error: file not found: {args.db_path}", file=sys.stderr)
        sys.exit(1)

    t0 = time.monotonic()
    try:
        conn = duckdb.connect(str(args.db_path), read_only=True)
    except duckdb.Error as exc:
        print(f"Error: cannot open {args.db_path}: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        data = _probe(conn, args.db_path, args.top)
    except duckdb.Error as exc:
        print(f"Error during inspection: {exc}", file=sys.stderr)
        conn.close()
        sys.exit(1)

    conn.close()
    elapsed = time.monotonic() - t0

    if args.as_json:
        # Make top serializable (list of dicts)
        data["top"] = [
            {"name": t, "estimated_bytes": b, "rows": r}
            for t, b, r in data["top"]
        ]
        print(json.dumps(data, indent=2, default=str))
    else:
        _print_report(data, args.top, args.settings)
        print(f"\nInspected in {elapsed:.2f}s.")
