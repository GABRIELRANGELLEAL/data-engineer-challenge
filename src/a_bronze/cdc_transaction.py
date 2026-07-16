import logging
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)


def load_transactions(
    batch_paths: list[str | Path],
    conn: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """
    Unions transaction batches and applies CDC to produce the current snapshot
    in raw_transactions.

    Handles schema drift automatically: batch_1 lacks `payment_method`;
    DuckDB's union_by_name fills it with NULL so both batches merge cleanly.
    """
    paths = [_to_posix(p) for p in batch_paths]

    for p in paths:
        if not Path(p).exists():
            raise FileNotFoundError(f"Input parquet file not found: {p}")

    paths_literal = _parquet_list(paths)

    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        total_raw: int = _conn.execute(
            f"SELECT COUNT(*) FROM read_parquet({paths_literal}, union_by_name=true)"
        ).fetchone()[0]

        null_ts_count: int = _conn.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet({paths_literal}, union_by_name=true)
            WHERE _timestamp IS NULL
        """).fetchone()[0]
        if null_ts_count > 0:
            raise ValueError(
                f"_timestamp is NULL in {null_ts_count} row(s); "
                "ORDER BY _timestamp DESC in the dedup window would silently mis-rank these rows"
            )

        _conn.execute(f"""
            CREATE OR REPLACE TABLE raw_transactions AS
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY transaction_id
                        ORDER BY _timestamp DESC
                    ) AS _rn
                FROM read_parquet({paths_literal}, union_by_name=true)
            )
            SELECT * EXCLUDE (_rn)
            FROM ranked
            WHERE _rn = 1
              AND Op != 'D'
        """)

        final: int = _conn.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0]
        removed = total_raw - final
        logger.info(
            "raw_transactions: %d raw rows → %d final rows (%d removed by dedup+deletes)",
            total_raw,
            final,
            removed,
        )
        return final

    finally:
        if owns_conn:
            _conn.close()


def _to_posix(path: str | Path) -> str:
    return str(Path(path).resolve()).replace("\\", "/")


def _parquet_list(paths: list[str]) -> str:
    quoted = ", ".join(f"'{p}'" for p in paths)
    return f"[{quoted}]"
