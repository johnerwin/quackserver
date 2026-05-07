"""
Shared helpers for duckdb-inspect and duckdb-compact CLI tools.

All functions take an open duckdb connection and return plain Python objects.
No duckdb exceptions escape these helpers — callers catch at their boundary.
"""

from __future__ import annotations

import re


def base_tables(conn) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' "
            "ORDER BY table_name"
        ).fetchall()
    ]


def view_names(conn) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='VIEW' "
            "ORDER BY table_name"
        ).fetchall()
    ]


def row_count(conn, table: str) -> int:
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def col_signatures(conn) -> dict[str, list[tuple[str, str]]]:
    """(column_name, data_type) pairs per table, ordered by position."""
    rows = conn.execute(
        "SELECT table_name, column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema='main' "
        "ORDER BY table_name, ordinal_position"
    ).fetchall()
    sig: dict[str, list] = {}
    for tname, col, dtype in rows:
        sig.setdefault(tname, []).append((col, dtype))
    return sig


def view_definitions(conn) -> dict[str, str]:
    # Use the same table_type filter as view_names() to exclude DuckDB internal views
    user_views = set(view_names(conn))
    rows = conn.execute(
        "SELECT table_name, view_definition "
        "FROM information_schema.views "
        "WHERE table_schema='main' "
        "ORDER BY table_name"
    ).fetchall()
    return {name: (defn or "") for name, defn in rows if name in user_views}


def estimated_sizes(conn) -> dict[str, int]:
    try:
        rows = conn.execute(
            "SELECT table_name, estimated_size FROM duckdb_tables() "
            "WHERE database_name = current_database() "
            "ORDER BY estimated_size DESC"
        ).fetchall()
        return {r[0]: (r[1] or 0) for r in rows}
    except Exception:
        return {}


def settings_snapshot(conn) -> dict[str, str]:
    rows = conn.execute(
        "SELECT name, value FROM duckdb_settings() ORDER BY name"
    ).fetchall()
    return {r[0]: str(r[1]) for r in rows}


def build_metadata(conn) -> dict[str, str] | None:
    """Read db_build_metadata if present, else None."""
    try:
        rows = conn.execute(
            "SELECT key, value FROM db_build_metadata ORDER BY key"
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return None


def dup_candidates(
    tables: list[str],
    sigs: dict[str, list],
    counts: dict[str, int],
) -> list[list[str]]:
    """Groups of tables with identical schema AND identical row count."""
    bucket: dict[str, list[str]] = {}
    for t in tables:
        key = str(tuple(sigs.get(t, [])))
        bucket.setdefault(key, []).append(t)

    result = []
    for group in bucket.values():
        if len(group) < 2:
            continue
        unique_counts = {counts[t] for t in group}
        if len(unique_counts) == 1:
            result.append(sorted(group))
    return result


def view_deps(vdefs: dict[str, str], tables: list[str]) -> dict[str, list[str]]:
    """For each view, which base tables does its definition reference?"""
    deps = {}
    for vname, vdef in vdefs.items():
        deps[vname] = [t for t in tables if vdef and re.search(r"\b" + re.escape(t) + r"\b", vdef)]
    return deps


def topo_sort_views(vdefs: dict[str, str]) -> list[str]:
    """Return view names in dependency order (each view after views it references)."""
    all_names = set(vdefs)
    deps: dict[str, set[str]] = {
        name: {
            other
            for other in all_names
            if other != name and vdefs.get(name) and re.search(r"\b" + re.escape(other) + r"\b", vdefs[name])
        }
        for name in all_names
    }
    remaining = {n: set(d) for n, d in deps.items()}
    order: list[str] = []
    ready = sorted(n for n, d in remaining.items() if not d)
    while ready:
        node = ready.pop(0)
        order.append(node)
        for candidate in sorted(all_names):
            if node in remaining.get(candidate, set()):
                remaining[candidate].remove(node)
                if not remaining[candidate]:
                    ready.append(candidate)
                    ready.sort()
    order.extend(sorted(all_names - set(order)))
    return order


# ---------------------------------------------------------------------------
# Hash fingerprint (order-independent)
# ---------------------------------------------------------------------------

HASH_SAMPLE_THRESHOLD = 5_000_000
HASH_SAMPLE_SIZE = 200_000


def table_hash(conn, table: str, count: int) -> int:
    """sum(hash(col1, col2, ...)) — order-independent row fingerprint.

    Uses modulo sampling for large tables: value-based, so the same rows are
    selected regardless of physical storage order (unlike RESERVOIR SAMPLE).
    """
    cols = [
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
            [table],
        ).fetchall()
    ]
    if not cols:
        return 0
    hash_expr = f"hash({', '.join(cols)})"
    if count > HASH_SAMPLE_THRESHOLD:
        modulus = max(2, count // HASH_SAMPLE_SIZE)
        sql = (
            f'SELECT sum({hash_expr}) FROM "{table}" '
            f'WHERE abs(hash("{cols[0]}")) % {modulus} = 0'
        )
    else:
        sql = f'SELECT sum({hash_expr}) FROM "{table}"'
    result = conn.execute(sql).fetchone()[0]
    return result if result is not None else 0


# ---------------------------------------------------------------------------
# DDL generation from information_schema (no EXPORT DATABASE needed)
# ---------------------------------------------------------------------------

def build_create_table_ddl(conn, table_name: str) -> str:
    """Generate a CREATE TABLE IF NOT EXISTS statement from information_schema.

    Captures: column names/types, NOT NULL, DEFAULT, PRIMARY KEY.
    Does not capture CHECK or FOREIGN KEY constraints (v1 limitation).
    """
    cols = conn.execute(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_schema='main' AND table_name=? "
        "ORDER BY ordinal_position",
        [table_name],
    ).fetchall()

    pk_row = conn.execute(
        "SELECT constraint_column_names FROM duckdb_constraints() "
        "WHERE schema_name='main' AND table_name=? AND constraint_type='PRIMARY KEY' "
        "LIMIT 1",
        [table_name],
    ).fetchone()
    pk_cols: list[str] = list(pk_row[0]) if pk_row else []
    col_order = [c[0] for c in cols]

    parts = []
    for col_name, data_type, is_nullable, col_default in cols:
        defn = f'    "{col_name}" {data_type}'
        if is_nullable == "NO" and col_name not in pk_cols:
            defn += " NOT NULL"
        if col_default is not None:
            defn += f" DEFAULT {col_default}"
        parts.append(defn)

    if pk_cols:
        ordered = [c for c in col_order if c in pk_cols]
        pk_str = ", ".join(f'"{c}"' for c in ordered)
        parts.append(f"    PRIMARY KEY ({pk_str})")

    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        + ",\n".join(parts)
        + "\n)"
    )


def fmt_size(n_bytes: int) -> str:
    if n_bytes >= 1e9:
        return f"{n_bytes / 1e9:.2f} GB"
    if n_bytes >= 1e6:
        return f"{n_bytes / 1e6:.0f} MB"
    if n_bytes >= 1e3:
        return f"{n_bytes / 1e3:.0f} KB"
    return f"{n_bytes} B"
