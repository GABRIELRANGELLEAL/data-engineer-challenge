"""
Bronze loader for reconciliation_results (raw Debezium-style CDC extract).

Lands the parquet as-is into raw_reconciliation_results — no CDC applied
here. Deduplication and Op='D' handling are the silver layer's job
(src.b_silver.cdc_reconc).
"""
import logging
import sys
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)


def load(
    parquet_path: str | Path,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """
    Loads reconciliation_results.parquet into raw_reconciliation_results.

    Args:
        parquet_path: Path to reconciliation_results.parquet.

    Returns:
        Number of rows landed (raw row count, before any CDC dedup).
    """
    path = _to_posix(parquet_path)

    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        _conn.execute(f"""
            CREATE OR REPLACE TABLE raw_reconciliation_results AS
            SELECT * FROM read_parquet('{path}')
        """)

        count: int = _conn.execute("SELECT COUNT(*) FROM raw_reconciliation_results").fetchone()[0]
        logger.info("Loaded %d raw rows into raw_reconciliation_results.", count)
        return count

    finally:
        if owns_conn:
            _conn.close()


def _to_posix(path: str | Path) -> str:
    return str(Path(path).resolve()).replace("\\", "/")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Load reconciliation_results.parquet into raw_reconciliation_results.")
    parser.add_argument("parquet", help="Path to reconciliation_results.parquet")
    args = parser.parse_args()

    try:
        n = load(args.parquet)
        print(f"Loaded {n} rows.")
    except Exception as exc:
        logger.error("Load failed: %s", exc)
        sys.exit(1)
