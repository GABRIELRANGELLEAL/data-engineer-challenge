import logging
import os
from collections import Counter
from datetime import datetime, timezone

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

# Configurable via environment variables — calibrate from historical baseline in production.
# Halt thresholds: pipeline stops and marks run FAILED if exceeded.
UNRECONCILED_MAX_RATE: float = float(os.environ.get("UNRECONCILED_MAX_RATE", "0.15"))
# Warn threshold: logged as WARNING but does not stop the pipeline.
MISMATCHED_WARN_RATE: float = float(os.environ.get("MISMATCHED_WARN_RATE", "0.10"))

# fmt: off
_RECONCILE_SQL = """
WITH
internal_window AS (
    SELECT
        transaction_id,
        merchant_id,
        CAST(amount AS DECIMAL(18, 2)) AS internal_amount
    FROM raw_transactions
    WHERE status = 'COMPLETED'
      AND CAST(created_at AS TIMESTAMPTZ)::DATE
          BETWEEN (CAST('{ref_date}' AS DATE) - INTERVAL '7 days')
          AND CAST('{ref_date}' AS DATE)
),
processor AS (
    SELECT
        transaction_id,
        merchant_id,
        amount AS processor_amount
    FROM raw_paysettler_settlements
    WHERE reference_date = CAST('{ref_date}' AS DATE)
      AND status = 'SETTLED'
)
SELECT
    COALESCE(i.transaction_id, p.transaction_id) AS transaction_id,
    COALESCE(i.merchant_id,    p.merchant_id)    AS merchant_id,
    i.internal_amount,
    p.processor_amount,
    CASE
        WHEN i.transaction_id IS NOT NULL AND p.transaction_id IS NOT NULL
            THEN ABS(i.internal_amount - p.processor_amount)
        ELSE NULL
    END AS difference,
    CASE
        WHEN i.transaction_id IS NULL THEN 'UNRECONCILED_PROCESSOR'
        WHEN p.transaction_id IS NULL THEN 'UNRECONCILED_INTERNAL'
        WHEN ABS(i.internal_amount - p.processor_amount) <= 0.01 THEN 'MATCHED'
        ELSE 'MISMATCHED'
    END AS category
FROM internal_window i
FULL OUTER JOIN processor p ON i.transaction_id = p.transaction_id
"""
# fmt: on


def reconcile(
    reference_date: str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """
    Runs the full reconciliation for a given reference_date.

    Each call creates a new run in silver_reconciliation_runs — this is
    intentional. Compliance requires an immutable audit trail, so re-running
    for the same date appends a new run rather than overwriting. Downstream
    consumers should query silver_reconciliation_results_current (a view that
    picks the latest completed run per reference_date), not the raw table.

    Args:
        reference_date: Business date to reconcile (YYYY-MM-DD).

    Returns:
        dict with run_id, status, total_transactions, and category breakdown.
    """
    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()
    run_id: int | None = None

    try:
        _assert_bronze_data_exists(_conn, reference_date)

        now = datetime.now(timezone.utc).isoformat()
        run_id = _conn.execute("SELECT nextval('seq_run_id')").fetchone()[0]

        _conn.execute(f"""
            INSERT INTO silver_reconciliation_runs
                (id, reference_date, file_name, status, total_transactions,
                 started_at, completed_at, created_at, source)
            VALUES (
                {run_id},
                CAST('{reference_date}' AS DATE),
                NULL,
                'IN_PROGRESS',
                NULL,
                CAST('{now}' AS TIMESTAMPTZ),
                NULL,
                CAST('{now}' AS TIMESTAMPTZ),
                'computed'
            )
        """)
        logger.info("Started run %d for reference_date=%s", run_id, reference_date)

        sql = _RECONCILE_SQL.format(ref_date=reference_date)

        # Compute category breakdown without materialising in Python
        cats_raw = _conn.execute(
            f"SELECT category, COUNT(*) FROM ({sql}) GROUP BY category"
        ).fetchall()
        cats = Counter({row[0]: row[1] for row in cats_raw})
        total = sum(cats.values())

        logger.info("reference_date=%s totals: %d rows | %s", reference_date, total, dict(cats))

        _run_quality_gate(cats, total, reference_date)

        completed_at = datetime.now(timezone.utc).isoformat()
        _conn.execute(f"""
            INSERT INTO silver_reconciliation_results
            SELECT
                nextval('seq_result_id') AS id,
                {run_id}                 AS run_id,
                transaction_id,
                merchant_id,
                category,
                internal_amount,
                processor_amount,
                difference,
                CAST('{completed_at}' AS TIMESTAMPTZ) AS created_at,
                'computed'              AS source
            FROM ({sql})
        """)

        _conn.execute(f"""
            UPDATE silver_reconciliation_runs
            SET status             = 'COMPLETED',
                completed_at       = CAST('{completed_at}' AS TIMESTAMPTZ),
                total_transactions = {total}
            WHERE id = {run_id}
        """)

        logger.info("Completed run %d: %d results inserted.", run_id, total)

        return {
            "run_id": run_id,
            "reference_date": reference_date,
            "status": "COMPLETED",
            "total_transactions": total,
            "categories": dict(cats),
        }

    except Exception:
        if run_id is not None:
            try:
                _conn.execute(f"""
                    UPDATE silver_reconciliation_runs
                    SET status = 'FAILED'
                    WHERE id = {run_id}
                """)
            except Exception:
                pass
        raise

    finally:
        if owns_conn:
            _conn.close()


def _assert_bronze_data_exists(conn: duckdb.DuckDBPyConnection, reference_date: str) -> None:
    count = conn.execute(f"""
        SELECT COUNT(*) FROM raw_paysettler_settlements
        WHERE reference_date = CAST('{reference_date}' AS DATE)
    """).fetchone()[0]

    if count == 0:
        raise ValueError(
            f"No settlement data in raw_paysettler_settlements for reference_date={reference_date}. "
            "Run bronze.settlement_loader.load() first."
        )


def _run_quality_gate(cats: Counter, total: int, reference_date: str) -> None:
    if total == 0:
        logger.warning("Empty reconciliation for reference_date=%s — no transactions found.", reference_date)
        return

    unrec_proc = cats.get("UNRECONCILED_PROCESSOR", 0) / total
    unrec_int  = cats.get("UNRECONCILED_INTERNAL", 0)  / total
    mismatch   = cats.get("MISMATCHED", 0)              / total

    if unrec_proc > UNRECONCILED_MAX_RATE:
        raise ValueError(
            f"Quality gate FAILED — UNRECONCILED_PROCESSOR rate {unrec_proc:.1%} "
            f"exceeds threshold {UNRECONCILED_MAX_RATE:.1%} for {reference_date}. "
            "Verify that the PaySettler CSV is for the correct date and that internal data is present."
        )
    if unrec_int > UNRECONCILED_MAX_RATE:
        raise ValueError(
            f"Quality gate FAILED — UNRECONCILED_INTERNAL rate {unrec_int:.1%} "
            f"exceeds threshold {UNRECONCILED_MAX_RATE:.1%} for {reference_date}. "
            "Check that raw_transactions covers the 7-day window ending on this date."
        )
    if mismatch > MISMATCHED_WARN_RATE:
        logger.warning(
            "MISMATCHED rate %.1f%% for %s exceeds warn threshold %.1f%% — alert ops team.",
            mismatch * 100,
            reference_date,
            MISMATCHED_WARN_RATE * 100,
        )
