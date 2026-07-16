"""
Gold layer builder.

Reads each .sql file under sql/ and executes it against the warehouse.
Views use CREATE OR REPLACE VIEW (always idempotent).
Tables use CREATE OR REPLACE TABLE (idempotent for local use; a production
deployment would need an explicit closed-week freeze process — see gold-schema.md).
"""
import logging
import sys
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent / "sql"

# Execution order matters: trend view depends on daily view.
_ARTIFACTS = [
    "ops_reconciliation_daily.sql",
    "ops_reconciliation_trend.sql",
    "cfo_weekly_summary.sql",
    "cfo_weekly_merchant_ranking.sql",
    "compliance_ledger.sql",
]


def build(conn: duckdb.DuckDBPyConnection | None = None) -> dict[str, str]:
    """
    Builds all gold layer artifacts from their SQL definitions.

    Safe to run multiple times — each artifact uses CREATE OR REPLACE.

    Args:
        conn: Optional DuckDB connection. If None, uses the default warehouse.

    Returns:
        Dict mapping artifact name → "ok".
    """
    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        results: dict[str, str] = {}
        for filename in _ARTIFACTS:
            sql = (_SQL_DIR / filename).read_text(encoding="utf-8")
            _conn.execute(sql)
            name = filename.removesuffix(".sql")
            logger.info("Built: gold_%s", name)
            results[name] = "ok"
        logger.info("Gold layer build complete (%d artifacts).", len(results))
        return results
    finally:
        if owns_conn:
            _conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    try:
        result = build()
        for name, status in result.items():
            print(f"  {status.upper()}  gold_{name}")
    except Exception as exc:
        logger.error("Gold build failed: %s", exc)
        sys.exit(1)
