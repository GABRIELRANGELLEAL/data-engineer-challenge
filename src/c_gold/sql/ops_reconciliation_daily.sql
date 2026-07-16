-- Grain: reference_date × category — always exactly one row per reference_date
-- present in silver_reconciliation_runs_latest, even if the latest run attempt
-- failed (category is NULL, txn_count 0, run_status carries the failure).
-- Winning-run policy for category data: latest COMPLETED run per date, via
-- silver_reconciliation_results_current (already filtered/enriched in silver).
CREATE OR REPLACE VIEW gold_ops_reconciliation_daily AS
WITH daily_totals AS (
    SELECT reference_date, COUNT(*) AS day_total
    FROM silver_reconciliation_results_current
    GROUP BY reference_date
)
SELECT
    lr.reference_date,
    lr.id                                            AS run_id,
    lr.status                                        AS run_status,
    rc.category,
    COUNT(rc.id)                                     AS txn_count,
    CASE
        WHEN dt.day_total IS NULL THEN NULL
        ELSE ROUND(COUNT(rc.id) * 1.0 / dt.day_total, 6)
    END                                               AS pct_of_total,
    SUM(rc.internal_amount)                          AS internal_amount_sum,
    SUM(rc.processor_amount)                         AS processor_amount_sum,
    SUM(rc.difference)                                AS abs_difference_sum
FROM silver_reconciliation_runs_latest lr
LEFT JOIN silver_reconciliation_results_current rc ON rc.reference_date = lr.reference_date
LEFT JOIN daily_totals dt ON dt.reference_date = lr.reference_date
GROUP BY lr.reference_date, lr.id, lr.status, rc.category, dt.day_total
ORDER BY lr.reference_date, rc.category
