"""
duckdb-compact - safe, validated compaction of any DuckDB file.

Usage:
    duckdb-compact --source path/to/source.duckdb --output path/to/compact.duckdb
    duckdb-compact --source path/to/source.duckdb --output path/to/compact.duckdb --dry-run

Phases
------
--dry-run (read-only, fast):
  1. Table inventory: row counts, estimated sizes, duplicate candidates
  2. View dependency map
  No files created.

Full compaction:
  1. Probe (as above)
  2. Hash baseline - order-independent sum(hash(*)) per table; modulo sample
     for large tables so the same rows are selected regardless of physical order
  3. Schema copy - DDL generated from information_schema (no EXPORT DATABASE)
  4. Data copy - ATTACH + INSERT INTO per table; fresh connection per table so
     the buffer pool resets completely between tables; large tables (>5M rows)
     split into 8 passes using hash partitioning to keep peak memory bounded
  5. View creation - topologically sorted so each view is created after any
     views it references (DuckDB EXPORT writes alphabetically, which breaks
     cross-view dependencies)
  6. VACUUM - consolidates row groups fragmented by multi-pass chunked inserts
  7. Validation - schema diff, row count diff, hash diff
  8. Build metadata - writes db_build_metadata to compact DB
  9. Swap instructions - printed; never executed automatically

Safety properties
-----------------
- Source opened read-only throughout; never modified.
- Output file written only after schema copy; data and validation follow.
- Swap is always manual: printed as instructions, not executed.
- All validation must pass before swap instructions are printed.
- Exit code 1 if validation fails; do NOT swap in that case.

Limitations (v1)
----------------
- CHECK and FOREIGN KEY constraints are not captured in DDL generation.
- Multi-schema databases: only 'main' schema is copied.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from quackserver.cli._duckdb_helpers import (
    HASH_SAMPLE_SIZE,
    HASH_SAMPLE_THRESHOLD,
    base_tables,
    build_create_table_ddl,
    build_metadata,
    col_signatures,
    dup_candidates,
    estimated_sizes,
    fmt_size,
    row_count,
    settings_snapshot,
    table_hash,
    topo_sort_views,
    view_definitions,
    view_deps,
    view_names,
)

_CHUNK_THRESHOLD = 5_000_000
_CHUNK_N = 8


# ---------------------------------------------------------------------------
# Phase 1: Probe
# ---------------------------------------------------------------------------


def _probe(conn, source_path: Path) -> dict:
    print("\n-- Phase 1: Probe --------------------------------------------------")
    tables = base_tables(conn)
    views = view_names(conn)
    sigs = col_signatures(conn)
    est = estimated_sizes(conn)
    vdefs = view_definitions(conn)

    counts: dict[str, int] = {}
    for t in tables:
        counts[t] = row_count(conn, t)

    total_rows = sum(counts.values())
    total_est = sum(est.get(t, 0) for t in tables)

    print(f"  Tables        : {len(tables)}")
    print(f"  Views         : {len(views)}")
    print(f"  Total rows    : {total_rows:,}")
    print(f"  Estimated live: {fmt_size(total_est)}")
    print(f"  File on disk  : {fmt_size(source_path.stat().st_size)}")

    sorted_sizes = sorted(est.items(), key=lambda x: x[1], reverse=True)[:10]
    if sorted_sizes:
        print("\n  Top tables by estimated size:")
        for tname, sz in sorted_sizes:
            print(
                f"    {tname:<50}  {fmt_size(sz):>9}  {counts.get(tname, 0):>12,} rows"
            )

    empty = [t for t, n in counts.items() if n == 0]
    print(f"\n  Empty tables ({len(empty)}): {empty or 'none'}")

    dups = dup_candidates(tables, sigs, counts)
    if dups:
        print(f"\n  WARN: Duplicate/alias candidates ({len(dups)} group(s)):")
        for group in dups:
            n = counts[group[0]]
            print(f"    {group}  - {n:,} rows, identical schema")
    else:
        print("\n  Duplicate/alias candidates: none")

    vdep = view_deps(vdefs, tables)
    if views:
        print(f"\n  Views ({len(views)}):")
        for v in topo_sort_views(vdefs):
            refs = vdep.get(v, [])
            print(f"    {v:<40}  -> {', '.join(refs) or '-'}")

    return {
        "tables": tables,
        "views": views,
        "sigs": sigs,
        "counts": counts,
        "vdefs": vdefs,
    }


# ---------------------------------------------------------------------------
# Phase 2: Hash baseline
# ---------------------------------------------------------------------------


def _hash_all(conn, tables: list[str], counts: dict[str, int]) -> dict[str, int]:
    print("\n-- Phase 2: Hash baseline ------------------------------------------")
    sampled = [t for t in tables if counts.get(t, 0) > HASH_SAMPLE_THRESHOLD]
    if sampled:
        print(
            f"  {len(sampled)} large table(s) will use ~{HASH_SAMPLE_SIZE:,}-row "
            f"modulo sample (value-based, order-independent)."
        )
    print(f"  Computing hash fingerprint for {len(tables)} tables...")
    hashes: dict[str, int] = {}
    t0 = time.monotonic()
    for i, t in enumerate(tables, 1):
        n = counts.get(t, 0)
        th = time.monotonic()
        h = table_hash(conn, t, n)
        hashes[t] = h
        elapsed_t = time.monotonic() - th
        if elapsed_t > 2.0:
            label = (
                f"modulo-sampled ~{HASH_SAMPLE_SIZE:,}"
                if n > HASH_SAMPLE_THRESHOLD
                else f"{n:,} rows"
            )
            print(f"  [{i}/{len(tables)}] {t} ({label}): {elapsed_t:.1f}s")
    print(f"  Hash baseline complete in {time.monotonic() - t0:.1f}s")
    return hashes


# ---------------------------------------------------------------------------
# Phase 3: Schema copy
# ---------------------------------------------------------------------------


def _copy_schema(src_conn, output_path: Path, probe_data: dict) -> None:
    print("\n-- Phase 3: Schema copy --------------------------------------------")
    tables = probe_data["tables"]
    vdefs = probe_data["vdefs"]

    # Use a single setup connection for DDL - no data yet
    with duckdb.connect(str(output_path)) as setup:
        print(f"  Creating {len(tables)} tables...")
        for t in tables:
            ddl = build_create_table_ddl(src_conn, t)
            setup.execute(ddl)

        view_order = topo_sort_views(vdefs)
        print(f"  Creating {len(view_order)} views (dependency order)...")
        for vname in view_order:
            vdef = vdefs.get(vname, "").strip().rstrip(";")
            if not vdef:
                continue
            # view_definition is the full CREATE VIEW statement; make it idempotent
            stmt = re.sub(
                r"(?i)^CREATE\s+VIEW\s+",
                "CREATE OR REPLACE VIEW ",
                vdef,
                count=1,
            )
            try:
                setup.execute(stmt)
            except duckdb.Error as exc:
                print(f"  WARN: view {vname!r} skipped: {exc}")


# ---------------------------------------------------------------------------
# Phase 4: Data copy (ATTACH + streaming INSERT, fresh connection per table)
# ---------------------------------------------------------------------------


def _copy_data(source_path: Path, output_path: Path, probe_data: dict) -> None:
    print("\n-- Phase 4: Data copy ----------------------------------------------")
    tables = probe_data["tables"]
    counts = probe_data["counts"]

    # Pre-fetch first column for each large table (for hash-partition key)
    first_col: dict[str, str] = {}
    with duckdb.connect(str(source_path), read_only=True) as fc:
        for t in tables:
            if counts.get(t, 0) > _CHUNK_THRESHOLD:
                row = fc.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='main' AND table_name=? "
                    "ORDER BY ordinal_position LIMIT 1",
                    [t],
                ).fetchone()
                if row:
                    first_col[t] = row[0]

    print(f"  Copying {len(tables)} tables (one fresh connection per table)...")
    t0 = time.monotonic()
    for i, t in enumerate(tables, 1):
        n = counts.get(t, 0)
        th = time.monotonic()

        if n > _CHUNK_THRESHOLD and t in first_col:
            key = first_col[t]
            print(
                f"  [{i}/{len(tables)}] \"{t}\": {n:,} rows - chunked x{_CHUNK_N} on \"{key}\""
            )
            for chunk in range(_CHUNK_N):
                with duckdb.connect(str(output_path)) as conn:
                    conn.execute("SET threads=1")
                    conn.execute("SET preserve_insertion_order=false")
                    conn.execute(f"ATTACH '{source_path}' AS src (READ_ONLY)")
                    conn.execute(
                        f'INSERT INTO "{t}" '
                        f'SELECT * FROM src.main."{t}" '
                        f'WHERE abs(hash("{key}")) % {_CHUNK_N} = {chunk}'
                    )
            elapsed_t = time.monotonic() - th
            print(f"  [{i}/{len(tables)}] \"{t}\": done in {elapsed_t:.1f}s")
        else:
            with duckdb.connect(str(output_path)) as conn:
                conn.execute("SET threads=1")
                conn.execute("SET preserve_insertion_order=false")
                conn.execute(f"ATTACH '{source_path}' AS src (READ_ONLY)")
                conn.execute(f'INSERT INTO "{t}" SELECT * FROM src.main."{t}"')
            elapsed_t = time.monotonic() - th
            if elapsed_t > 5.0:
                print(f"  [{i}/{len(tables)}] \"{t}\": {elapsed_t:.1f}s")

    print(f"  Data copy complete in {time.monotonic() - t0:.1f}s")


# ---------------------------------------------------------------------------
# Phase 5: VACUUM (consolidate row groups from multi-pass chunked inserts)
# ---------------------------------------------------------------------------


def _vacuum(output_path: Path, tables: list[str]) -> None:
    print("\n-- Phase 5: VACUUM -------------------------------------------------")
    t0 = time.monotonic()
    before = output_path.stat().st_size
    for i, t in enumerate(tables, 1):
        th = time.monotonic()
        with duckdb.connect(str(output_path)) as conn:
            conn.execute("SET threads=1")
            conn.execute(f'VACUUM "{t}"')
        elapsed_t = time.monotonic() - th
        if elapsed_t > 5.0:
            print(f"  [{i}/{len(tables)}] \"{t}\": {elapsed_t:.1f}s")
    after = output_path.stat().st_size
    saved = before - after
    print(
        f"  VACUUM complete in {time.monotonic() - t0:.1f}s  "
        f"{fmt_size(before)} -> {fmt_size(after)}"
        + (f"  (saved {fmt_size(saved)})" if saved > 0 else "")
    )


# ---------------------------------------------------------------------------
# Phase 6: Validation
# ---------------------------------------------------------------------------


def _validate(
    probe_data: dict,
    src_hashes: dict[str, int],
    dst_conn,
) -> bool:
    print("\n-- Phase 6: Validation ---------------------------------------------")
    tables = probe_data["tables"]
    failures: list[str] = []

    # Schema diff
    print("  Schema diff...")
    dst_sigs = col_signatures(dst_conn)
    for t in tables:
        src_sig = probe_data["sigs"].get(t, [])
        dst_sig = dst_sigs.get(t)
        if dst_sig is None:
            failures.append(f"SCHEMA: table missing in output: {t}")
        elif src_sig != dst_sig:
            failures.append(f"SCHEMA: column mismatch in {t}")

    # Row counts
    print("  Row count diff...")
    for t in tables:
        src_n = probe_data["counts"][t]
        dst_n = row_count(dst_conn, t)
        if src_n != dst_n:
            failures.append(f"ROWCOUNT: {t}  src={src_n:,}  dst={dst_n:,}")

    # Hashes
    print("  Hash diff...")
    for t in tables:
        dst_h = table_hash(dst_conn, t, probe_data["counts"][t])
        if src_hashes[t] != dst_h:
            failures.append(
                f"HASH: {t}  src={src_hashes[t]}  dst={dst_h}"
            )

    # Views
    print("  View definition diff...")
    dst_vdefs = view_definitions(dst_conn)
    for vname, src_def in probe_data["vdefs"].items():
        dst_def = dst_vdefs.get(vname)
        if dst_def is None:
            failures.append(f"VIEW MISSING: {vname}")
        elif src_def.strip() != dst_def.strip():
            failures.append(f"VIEW CHANGED: {vname}")

    if failures:
        print(f"\n  FAIL - {len(failures)} issue(s):")
        for f in failures:
            print(f"    {f}")
        return False

    print("\n  PASS - all checks passed.")
    return True


# ---------------------------------------------------------------------------
# Phase 7: Build metadata
# ---------------------------------------------------------------------------


def _write_build_metadata(
    dst_conn,
    source_path: Path,
    probe_data: dict,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    sigs = probe_data["sigs"]
    sig_str = json.dumps(
        {t: sigs[t] for t in sorted(sigs)}, sort_keys=True
    )
    fingerprint = hashlib.sha256(sig_str.encode()).hexdigest()[:16]
    total_rows = sum(probe_data["counts"].values())

    dst_conn.execute("""
        CREATE TABLE IF NOT EXISTS db_build_metadata (
            key         VARCHAR PRIMARY KEY,
            value       VARCHAR NOT NULL,
            recorded_at VARCHAR NOT NULL
        )
    """)
    entries = [
        ("compacted_at", now),
        ("source_db_path", str(source_path.resolve())),
        ("source_db_size", fmt_size(source_path.stat().st_size)),
        ("total_rows", str(total_rows)),
        ("table_count", str(len(probe_data["tables"]))),
        ("view_count", str(len(probe_data["views"]))),
        ("schema_fingerprint", fingerprint),
    ]
    for key, value in entries:
        dst_conn.execute(
            """
            INSERT INTO db_build_metadata (key, value, recorded_at)
            VALUES (?, ?, ?)
            ON CONFLICT (key) DO UPDATE
              SET value = excluded.value, recorded_at = excluded.recorded_at
            """,
            [key, value, now],
        )
    print(
        f"\n-- Phase 7: Build metadata -----------------------------------------"
        f"\n  db_build_metadata written  fingerprint={fingerprint}"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="duckdb-compact",
        description="Compact any DuckDB file via ATTACH streaming copy with full validation.",
    )
    parser.add_argument(
        "--source",
        required=True,
        type=Path,
        metavar="PATH",
        help="Path to the source DuckDB file (opened read-only).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="PATH",
        help="Destination path for the compacted DuckDB file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Probe only: inventory, duplicates, view deps. No files created.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace output file if it already exists.",
    )
    args = parser.parse_args(argv)

    if not args.source.exists():
        print(f"Error: source not found: {args.source}", file=sys.stderr)
        sys.exit(1)

    if args.output.exists() and not args.dry_run:
        if not args.overwrite:
            print(
                f"Error: {args.output} already exists. Pass --overwrite to replace it.",
                file=sys.stderr,
            )
            sys.exit(1)
        args.output.unlink()
        wal = Path(str(args.output) + ".wal")
        if wal.exists():
            wal.unlink()

    t_total = time.monotonic()
    print(f"Source : {args.source.resolve()}  ({fmt_size(args.source.stat().st_size)})")
    if not args.dry_run:
        print(f"Output : {args.output.resolve()}")

    try:
        src = duckdb.connect(str(args.source), read_only=True)
    except duckdb.Error as exc:
        print(f"Error: cannot open source: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        probe_data = _probe(src, args.source)
    except duckdb.Error as exc:
        print(f"Error during probe: {exc}", file=sys.stderr)
        src.close()
        sys.exit(1)

    if args.dry_run:
        src.close()
        print(f"\nDry-run complete in {time.monotonic() - t_total:.1f}s.")
        print("Run without --dry-run to perform compaction.")
        return

    src_hashes = _hash_all(src, probe_data["tables"], probe_data["counts"])

    # Schema copy uses src_conn for DDL introspection
    _copy_schema(src, args.output, probe_data)
    src.close()

    # Data copy: each table opens its own connection to source (ATTACH)
    _copy_data(args.source, args.output, probe_data)

    # VACUUM to consolidate row groups from chunked inserts
    _vacuum(args.output, probe_data["tables"])

    # Validation
    try:
        dst = duckdb.connect(str(args.output))
    except duckdb.Error as exc:
        print(f"Error: cannot open output for validation: {exc}", file=sys.stderr)
        sys.exit(1)

    passed = _validate(probe_data, src_hashes, dst)

    if passed:
        _write_build_metadata(dst, args.source, probe_data)

    dst.close()

    # Result summary
    elapsed = time.monotonic() - t_total
    src_size = args.source.stat().st_size
    dst_size = args.output.stat().st_size if args.output.exists() else 0
    savings = src_size - dst_size
    pct = savings / src_size * 100 if src_size else 0

    print(f"\n-- Result ----------------------------------------------------------")
    print(f"  Source   : {fmt_size(src_size)}")
    print(f"  Output   : {fmt_size(dst_size)}")
    print(f"  Savings  : {fmt_size(savings)}  ({pct:.0f}%)")
    print(f"  Duration : {elapsed:.1f}s")

    if not passed:
        print("\n  *** Validation failed. Do NOT swap. Investigate the failures above. ***")
        sys.exit(1)

    print(f"""
  Validation passed. To complete the swap (run manually when ready):

    # Unix / macOS / Linux:
    mv {args.source} {args.source}.bak
    mv {args.output} {args.source}

    # Windows PowerShell:
    Rename-Item '{args.source}' '{args.source}.bak'
    Rename-Item '{args.output}' '{args.source}'

  To clean up after confirming the compact DB is good:

    # Unix:
    rm {args.source}.bak

    # Windows:
    Remove-Item '{args.source}.bak'
""")
