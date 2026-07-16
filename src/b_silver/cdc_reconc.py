"""
Silver seeder for reconciliation_runs / reconciliation_results.

Applies CDC (latest-wins by _timestamp, excludes Op='D') on top of the bronze
raw extracts (raw_reconciliation_runs, raw_reconciliation_results) and
(re)builds silver_reconciliation_runs / silver_reconciliation_results from
scratch. Idempotent — safe to run as many times as needed; each run fully
replaces both tables based on whatever is currently in bronze.

The curated "current" view (winning run + merchant enrichment) is built
separately by src.b_silver.build, since it depends on silver_enterprise_company
also being seeded.
"""
import logging
import sys

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_REQUIRED_RAW_TABLES = ("raw_reconciliation_runs", "raw_reconciliation_results")


def seed(conn: duckdb.DuckDBPyConnection | None = None) -> dict:
    """
    (Re)builds silver_reconciliation_runs / silver_reconciliation_results from
    the bronze raw extracts (raw_reconciliation_runs, raw_reconciliation_results).

    Applies CDC (latest-wins by _timestamp, excludes Op='D').

    Requires src.a_bronze.reconciliation_runs.load and
    src.a_bronze.reconciliation_results.load to have already populated the
    raw tables.
    """
    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        _assert_raw_tables_exist(_conn)

        _conn.execute("""
            CREATE OR REPLACE TABLE silver_reconciliation_runs AS
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC) AS _rn
                FROM raw_reconciliation_runs
            )
            SELECT
                id,
                CAST(reference_date AS DATE)      AS reference_date,
                file_name,
                status,
                CAST(total_transactions AS INTEGER) AS total_transactions,
                CAST(started_at   AS TIMESTAMPTZ) AS started_at,
                CAST(completed_at AS TIMESTAMPTZ) AS completed_at,
                CAST(created_at   AS TIMESTAMPTZ) AS created_at,
                'historical_backfill' AS source
            FROM ranked
            WHERE _rn = 1 AND Op != 'D'
        """)
        _conn.execute("ALTER TABLE silver_reconciliation_runs ADD PRIMARY KEY (id)")

        _conn.execute("""
            CREATE OR REPLACE TABLE silver_reconciliation_results AS
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC) AS _rn
                FROM raw_reconciliation_results
            )
            SELECT
                id,
                run_id,
                transaction_id,
                merchant_id,
                category,
                CAST(internal_amount  AS DECIMAL(18, 2)) AS internal_amount,
                CAST(processor_amount AS DECIMAL(18, 2)) AS processor_amount,
                CAST(difference       AS DECIMAL(18, 2)) AS difference,
                CAST(created_at AS TIMESTAMPTZ) AS created_at,
                'historical_backfill' AS source
            FROM ranked
            WHERE _rn = 1 AND Op != 'D'
        """)
        _conn.execute("ALTER TABLE silver_reconciliation_results ADD PRIMARY KEY (id)")

        runs_loaded = _conn.execute(
            "SELECT COUNT(*) FROM silver_reconciliation_runs"
        ).fetchone()[0]
        results_loaded = _conn.execute(
            "SELECT COUNT(*) FROM silver_reconciliation_results"
        ).fetchone()[0]

        logger.info(
            "Seed complete: %d runs, %d results.",
            runs_loaded,
            results_loaded,
        )

        return {
            "runs_loaded": runs_loaded,
            "results_loaded": results_loaded,
        }

    finally:
        if owns_conn:
            _conn.close()


def _assert_raw_tables_exist(conn: duckdb.DuckDBPyConnection) -> None:
    for raw_table in _REQUIRED_RAW_TABLES:
        exists: bool = conn.execute(
            "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = ?",
            [raw_table],
        ).fetchone()[0]
        if not exists:
            raise RuntimeError(
                f"{raw_table} not found. Run its bronze loader (src.a_bronze.reconciliation_runs "
                "/ reconciliation_results) before seeding silver."
            )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    try:
        result = seed()
        print(f"Loaded {result['runs_loaded']} runs, {result['results_loaded']} results.")
    except Exception as exc:
        logger.error("Seed failed: %s", exc)
        sys.exit(1)
