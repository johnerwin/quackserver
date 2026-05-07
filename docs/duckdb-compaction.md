# Compacting a Large DuckDB File Without OOMing

## The problem

DuckDB has no `VACUUM FULL`. After months of active development — schema iterations, reruns, backfills, heavy UPSERT activity — a database accumulates dead space. The documented compaction approach is:

```sql
EXPORT DATABASE '/tmp/export' (FORMAT PARQUET, COMPRESSION zstd);
-- ... edit schema.sql if needed ...
IMPORT DATABASE '/tmp/export';
```

On a small database, this works fine. On a large one, it will OOM your machine.

We hit this on a 43 GB DuckDB file. Estimated live data: 688 MB. The process was killed by the OS at 12.5 GB of RAM — every time, regardless of tuning.

---

## What we tried

Every knob that should matter:

```python
conn.execute("SET memory_limit='8GB'")
conn.execute("SET threads=1")
conn.execute("SET preserve_insertion_order=false")
conn.execute("SET temp_directory='/fast/ssd'")
```

None of it helped. The OOM happened consistently at the same point in the import.

---

## Why IMPORT DATABASE OOMs

The failure has two compounding causes.

**Cause 1: Parquet decompression amplification.**
`EXPORT DATABASE` writes compressed Parquet files. During import, DuckDB decompresses them before inserting. A zstd-compressed Parquet file typically expands 10–20x on decompression. Our 688 MB export expanded to roughly 8–12 GB of in-memory buffers. `memory_limit` does not bound this — decompression buffers are allocated outside DuckDB's tracked allocator.

**Cause 2: Everything in one transaction.**
`IMPORT DATABASE` loads all tables in a single operation. Dirty pages accumulate across every table without any flush boundary between them. By the time the largest tables are processed, the dirty page set plus the decompression buffers exceed available RAM.

The practical amplification factor we observed: **~18x** the compressed export size.

---

## The insight: `close()` is a resource boundary

When you close a DuckDB connection, it:

1. Writes all dirty pages to disk (full checkpoint)
2. Releases every cached block from the buffer pool
3. Frees all connection-scoped memory

`FORCE CHECKPOINT` does step 1 but not steps 2–3. It flushes dirty pages but leaves the buffer pool populated. The next table copy starts with a full buffer pool and immediately competes for RAM.

`close()` is a guaranteed reset. Opening a fresh connection per table means each table copy starts with an empty buffer pool.

---

## The reliable pattern

Skip EXPORT/IMPORT entirely. Use `ATTACH` to stream data directly between two DuckDB files:

```python
import duckdb
from pathlib import Path

src_path = Path("source.duckdb")
dst_path = Path("compact.duckdb")

# Get table list from source
with duckdb.connect(str(src_path), read_only=True) as src:
    tables = [
        r[0] for r in src.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' "
            "ORDER BY table_name"
        ).fetchall()
    ]

# Copy each table in its own connection — buffer pool resets between tables
for table in tables:
    with duckdb.connect(str(dst_path)) as conn:
        conn.execute("SET threads=1")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute(f"ATTACH '{src_path}' AS src (READ_ONLY)")
        conn.execute(f'INSERT INTO "{table}" SELECT * FROM src.main."{table}"')
    # conn.close() here — full buffer pool release before next table
```

No Parquet decompression. DuckDB streams blocks directly between the two database files. Peak memory stays bounded to roughly the size of the largest single table's working set.

---

## Chunking for very large tables

For tables with more than ~5 million rows, a single `INSERT INTO ... SELECT *` can still spike memory. Split it into passes using hash partitioning:

```python
CHUNK_N = 8  # number of passes

for chunk in range(CHUNK_N):
    with duckdb.connect(str(dst_path)) as conn:
        conn.execute("SET threads=1")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute(f"ATTACH '{src_path}' AS src (READ_ONLY)")
        conn.execute(
            f'INSERT INTO "{table}" '
            f'SELECT * FROM src.main."{table}" '
            f'WHERE abs(hash("{key_col}")) % {CHUNK_N} = {chunk}'
        )
```

`abs(hash(key_col)) % N` is value-based: the same row always maps to the same chunk regardless of physical storage order. This matters for validation — see below.

After a chunked copy, run `VACUUM` to consolidate the row groups created by multiple passes:

```python
with duckdb.connect(str(dst_path)) as conn:
    conn.execute(f'VACUUM "{table}"')
```

---

## Validating the result

Before swapping the compact file into production, verify it matches the source exactly. An order-independent hash fingerprint works well:

```python
def table_hash(conn, table: str) -> int:
    cols = [
        r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='main' AND table_name=? "
            "ORDER BY ordinal_position",
            [table],
        ).fetchall()
    ]
    result = conn.execute(
        f"SELECT sum(hash({', '.join(cols)})) FROM \"{table}\""
    ).fetchone()[0]
    return result or 0
```

`sum(hash(...))` is commutative — it produces the same value regardless of row order, which matters because the chunked copy does not preserve insertion order.

For large tables (>5M rows), compute the hash on a modulo sample rather than a full scan:

```python
modulus = max(2, row_count // 200_000)  # target ~200k rows
sql = (
    f'SELECT sum(hash({", ".join(cols)})) FROM "{table}" '
    f'WHERE abs(hash("{key_col}")) % {modulus} = 0'
)
```

Use modulo sampling, not `RESERVOIR SAMPLE`. Reservoir sampling is position-dependent: if rows end up in different physical order after the copy (likely with `preserve_insertion_order=false`), a reservoir sample will pick different rows and produce different hashes even when the data is correct. Modulo sampling is purely value-based and picks the same rows regardless of order.

---

## The view ordering bug in EXPORT DATABASE

One other landmine: `EXPORT DATABASE` writes `CREATE VIEW` statements in `schema.sql` in alphabetical order. If view `a` depends on view `b`, and `a` sorts before `b` alphabetically, `IMPORT DATABASE` will fail when it tries to create `a` before `b` exists.

The fix is to sort views in dependency order before creating them (Kahn's topological sort). Since the ATTACH approach doesn't use `schema.sql`, this is only relevant if you use EXPORT/IMPORT for some other reason.

---

## Results

Applied to a 43 GB DuckDB file:

| Metric | Before | After |
|---|---|---|
| File size on disk | 43 GB | 27 GB |
| Cold-start query time | 60–120s | 0.83s |
| Peak RAM during compaction | < 2 GB | — |
| Duration | — | ~45 min |

The cold-start improvement was the unexpected win. A freshly compacted file has no WAL fragmentation and fully consolidated row groups — the first query after open hits clean pages and runs fast.

---

## The `duckdb-compact` tool

This pattern is implemented in [`quackserver/cli/compact.py`](../quackserver/cli/compact.py) as a general-purpose CLI tool that works on any DuckDB file:

```bash
# Dry run: probe only, no files written
duckdb-compact --source path/to/source.duckdb --output path/to/compact.duckdb --dry-run

# Full compaction with validation
duckdb-compact --source path/to/source.duckdb --output path/to/compact.duckdb
```

Phases: probe → hash baseline → schema copy → ATTACH streaming copy → VACUUM → validation → swap instructions. The swap is always printed as instructions and never executed automatically.

---

## Summary

| Approach | Peak RAM | Reliable at scale |
|---|---|---|
| `IMPORT DATABASE` from Parquet | ~18x compressed size | No |
| Single-connection ATTACH copy | ~1x largest table | Marginal for very large tables |
| **Per-table ATTACH + chunking** | **Bounded, predictable** | **Yes** |

The key principle: treat `connect()` / `close()` as a resource lifecycle boundary, not just an API call. Each table gets its own connection so the buffer pool resets completely between tables. For tables too large to copy in one pass, hash partitioning keeps each pass within bounds.

No Parquet. No EXPORT. No surprises.
