"""
Generic data health reporting for warehouse tables.

Introspects a table's schema (no per-table config needed) and reports row
count, null rates per column, duplicate primary keys (if pk_cols given), and
the min/max of its first date/timestamp column. Meant to be printed right
after a load step so drift/quality issues surface immediately instead of
being discovered downstream in the gold layer.
"""
import duckdb

_DATE_TYPE_PREFIXES = ("DATE", "TIMESTAMP")
# Fallback for bronze tables where date/timestamp columns haven't been CAST
# yet and still show up as VARCHAR (e.g. raw_transactions._timestamp).
_DATE_NAME_HINTS = ("_timestamp", "created_at", "updated_at", "settled_at", "reference_date")


def profile_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    pk_cols: list[str] | None = None,
) -> dict:
    """
    Computes health metrics for `table`. Returns a dict; see print_table_health
    for the human-readable rendering of the same data.
    """
    columns = conn.execute(f"DESCRIBE {table}").fetchall()
    col_names = [c[0] for c in columns]
    col_types = {c[0]: c[1] for c in columns}

    row_count: int = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    nulls: dict[str, int] = {}
    if row_count > 0 and col_names:
        null_exprs = ", ".join(f'COUNT(*) - COUNT("{c}") AS "{c}"' for c in col_names)
        null_row = conn.execute(f"SELECT {null_exprs} FROM {table}").fetchone()
        nulls = dict(zip(col_names, null_row))

    dup_count = None
    if pk_cols and row_count > 0:
        pk_list = ", ".join(f'"{c}"' for c in pk_cols)
        dup_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {pk_list} FROM {table} GROUP BY {pk_list} HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

    date_col = next(
        (c for c, t in col_types.items() if t.upper().startswith(_DATE_TYPE_PREFIXES)),
        None,
    )
    if date_col is None:
        date_col = next((c for c in col_names if c in _DATE_NAME_HINTS), None)

    date_range = None
    if date_col and row_count > 0:
        try:
            date_range = conn.execute(
                f'SELECT MIN(TRY_CAST("{date_col}" AS TIMESTAMP)), MAX(TRY_CAST("{date_col}" AS TIMESTAMP)) FROM {table}'
            ).fetchone()
        except duckdb.Error:
            date_range = None

    return {
        "table": table,
        "row_count": row_count,
        "column_count": len(col_names),
        "pk_cols": pk_cols,
        "duplicate_pk_count": dup_count,
        "date_col": date_col,
        "date_range": date_range,
        "nulls": {c: n for c, n in nulls.items() if n > 0},
    }


def print_table_health(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    pk_cols: list[str] | None = None,
) -> dict:
    """Prints a health report for `table` and returns the underlying metrics dict."""
    stats = profile_table(conn, table, pk_cols=pk_cols)
    row_count = stats["row_count"]

    print(f"\n=== Data Health: {table} ===")
    print(f"Rows:      {row_count:,}")
    print(f"Columns:   {stats['column_count']}")

    if pk_cols:
        dup = stats["duplicate_pk_count"]
        status = "OK" if dup == 0 else f"WARNING — {dup} duplicate key(s)"
        print(f"PK check ({', '.join(pk_cols)}): {status}")

    if stats["date_col"]:
        start, end = stats["date_range"]
        print(f"Date range ({stats['date_col']}): {start} -> {end}")

    nulls = stats["nulls"]
    if nulls:
        print("Nulls:")
        for col, n in sorted(nulls.items(), key=lambda kv: -kv[1]):
            pct = (n / row_count * 100) if row_count else 0.0
            print(f"  {col:<24} {n:>10,} ({pct:.1f}%)")
    else:
        print("Nulls:     none")

    print("=" * 40)
    return stats
