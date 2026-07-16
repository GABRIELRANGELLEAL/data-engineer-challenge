-- Grain: run × category. Unlike gold_ops_reconciliation_daily (winning-run-per-date
-- policy), this surfaces EVERY run attempt for a reference_date, whatever its status —
-- including reruns superseded by a later COMPLETED run and runs that failed before
-- producing any results.
-- LEFT JOINs from silver_reconciliation_runs (not silver_reconciliation_results_current,
-- which only ever contains the winning COMPLETED run per date) so a failed/empty run
-- still gets exactly one row, with category/txn_count/pct_of_total as NULL/0.
CREATE OR REPLACE VIEW gold_ops_run_history AS
WITH run_category_counts AS (
    SELECT run_id, category, COUNT(*) AS txn_count
    FROM silver_reconciliation_results
    GROUP BY run_id, category
),
run_totals AS (
    SELECT run_id, COUNT(*) AS total_txn_count
    FROM silver_reconciliation_results
    GROUP BY run_id
)
SELECT
    run.id                              AS run_id,
    run.reference_date,
    run.file_name,
    run.status                          AS run_status,
    run.total_transactions,
    run.started_at,
    run.completed_at,
    rc.category,
    COALESCE(rc.txn_count, 0)           AS txn_count,
    CASE
        WHEN rt.total_txn_count IS NULL OR rt.total_txn_count = 0 THEN NULL
        ELSE ROUND(rc.txn_count * 1.0 / rt.total_txn_count, 6)
    END                                  AS pct_of_total
FROM silver_reconciliation_runs run
LEFT JOIN run_category_counts rc ON rc.run_id = run.id
LEFT JOIN run_totals rt ON rt.run_id = run.id
ORDER BY run.reference_date DESC, run.started_at DESC, run.id, rc.category
