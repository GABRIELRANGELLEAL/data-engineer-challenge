import logging
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS silver_reconciliation_runs (
    id                 BIGINT PRIMARY KEY,
    reference_date     DATE NOT NULL,
    file_name          VARCHAR,
    status             VARCHAR NOT NULL,
    total_transactions INTEGER,
    started_at         TIMESTAMPTZ,
    completed_at       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL,
    source             VARCHAR NOT NULL
)
"""

_CREATE_RESULTS = """
CREATE TABLE IF NOT EXISTS silver_reconciliation_results (
    id               BIGINT PRIMARY KEY,
    run_id           BIGINT NOT NULL,
    transaction_id   VARCHAR NOT NULL,
    merchant_id      VARCHAR,
    category         VARCHAR NOT NULL,
    internal_amount  DECIMAL(18, 2),
    processor_amount DECIMAL(18, 2),
    difference       DECIMAL(18, 2),
    created_at       TIMESTAMPTZ NOT NULL,
    source           VARCHAR NOT NULL
)
"""

_CREATE_CURRENT_VIEW = """
CREATE OR REPLACE VIEW silver_reconciliation_results_current AS
WITH latest_runs AS (
    SELECT reference_date, MAX(id) AS latest_run_id
    FROM silver_reconciliation_runs
    WHERE status = 'COMPLETED'
    GROUP BY reference_date
)
SELECT rr.*
FROM silver_reconciliation_results rr
JOIN latest_runs lr ON rr.run_id = lr.latest_run_id
"""


def seed(
    runs_parquet: str | Path,
    results_parquet: str | Path,
    force: bool = False,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """
    One-time historical seed for silver reconciliation tables.

    Applies CDC (latest-wins by _timestamp, excludes Op='D') before inserting,
    then creates DuckDB sequences so reconcile.py can generate collision-free IDs.

    Args:
        runs_parquet:    Path to reconciliation_runs.parquet.
        results_parquet: Path to reconciliation_results.parquet.
        force:           Drop and recreate silver tables if they already have rows.
    """
    runs_path = _to_posix(runs_parquet)
    results_path = _to_posix(results_parquet)

    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        _conn.execute(_CREATE_RUNS)
        _conn.execute(_CREATE_RESULTS)

        existing = _conn.execute(
            "SELECT COUNT(*) FROM silver_reconciliation_runs"
        ).fetchone()[0]

        if existing > 0 and not force:
            raise RuntimeError(
                f"silver_reconciliation_runs already has {existing} rows. "
                "Pass force=True to drop and re-seed (existing computed rows will be lost)."
            )

        if force and existing > 0:
            logger.warning("--force: dropping and recreating silver reconciliation tables.")
            _conn.execute("DROP TABLE IF EXISTS silver_reconciliation_results")
            _conn.execute("DROP TABLE IF EXISTS silver_reconciliation_runs")
            _conn.execute(_CREATE_RUNS)
            _conn.execute(_CREATE_RESULTS)

        _conn.execute(f"""
            INSERT INTO silver_reconciliation_runs
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC) AS _rn
                FROM read_parquet('{runs_path}')
            )
            SELECT
                id,
                CAST(reference_date AS DATE),
                file_name,
                status,
                CAST(total_transactions AS INTEGER),
                CAST(started_at   AS TIMESTAMPTZ),
                CAST(completed_at AS TIMESTAMPTZ),
                CAST(created_at   AS TIMESTAMPTZ),
                'historical_backfill' AS source
            FROM ranked
            WHERE _rn = 1 AND Op != 'D'
        """)

        _conn.execute(f"""
            INSERT INTO silver_reconciliation_results
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC) AS _rn
                FROM read_parquet('{results_path}')
            )
            SELECT
                id,
                run_id,
                transaction_id,
                merchant_id,
                category,
                CAST(internal_amount  AS DECIMAL(18, 2)),
                CAST(processor_amount AS DECIMAL(18, 2)),
                CAST(difference       AS DECIMAL(18, 2)),
                CAST(created_at AS TIMESTAMPTZ),
                'historical_backfill' AS source
            FROM ranked
            WHERE _rn = 1 AND Op != 'D'
        """)

        max_run_id = _conn.execute(
            "SELECT MAX(id) FROM silver_reconciliation_runs"
        ).fetchone()[0]
        max_result_id = _conn.execute(
            "SELECT MAX(id) FROM silver_reconciliation_results"
        ).fetchone()[0]

        # Sequences used by reconcile.py to generate collision-free IDs
        _conn.execute("DROP SEQUENCE IF EXISTS seq_run_id")
        _conn.execute(f"CREATE SEQUENCE seq_run_id START {max_run_id + 1}")
        _conn.execute("DROP SEQUENCE IF EXISTS seq_result_id")
        _conn.execute(f"CREATE SEQUENCE seq_result_id START {max_result_id + 1}")

        _conn.execute(_CREATE_CURRENT_VIEW)

        runs_loaded = _conn.execute(
            "SELECT COUNT(*) FROM silver_reconciliation_runs"
        ).fetchone()[0]
        results_loaded = _conn.execute(
            "SELECT COUNT(*) FROM silver_reconciliation_results"
        ).fetchone()[0]

        logger.info(
            "Seed complete: %d runs, %d results. seq_run_id starts at %d, seq_result_id at %d.",
            runs_loaded,
            results_loaded,
            max_run_id + 1,
            max_result_id + 1,
        )

        return {
            "runs_loaded": runs_loaded,
            "results_loaded": results_loaded,
            "seq_run_id_start": max_run_id + 1,
            "seq_result_id_start": max_result_id + 1,
        }

    finally:
        if owns_conn:
            _conn.close()


def _to_posix(path: str | Path) -> str:
    return str(Path(path).resolve()).replace("\\", "/")
