"""
Silver layer curated-view builder.

Reads each .sql file under sql/ and executes it against the warehouse.
These views filter and enrich the CDC-seeded silver tables (winning-run
selection, merchant identity enrichment) — they don't aggregate by product
dimension, that's the gold layer's job.

Run after silver_reconciliation_runs/results and silver_enterprise_company
have been seeded (src.b_silver.cdc_reconc, src.b_silver.cdc_company).
"""
import logging
import sys
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent / "sql"

# Execution order matters: results_current doesn't depend on runs_latest, but
# both must run after the base silver tables are populated.
_ARTIFACTS = [
    "silver_reconciliation_runs_latest.sql",
    "silver_reconciliation_results_current.sql",
]


def build(conn: duckdb.DuckDBPyConnection | None = None) -> dict[str, str]:
    """
    Builds all curated silver views from their SQL definitions.

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
            logger.info("Built: %s", name)
            results[name] = "ok"
        logger.info("Silver curated-view build complete (%d artifacts).", len(results))
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
            print(f"  {status.upper()}  {name}")
    except Exception as exc:
        logger.error("Silver build failed: %s", exc)
        sys.exit(1)
