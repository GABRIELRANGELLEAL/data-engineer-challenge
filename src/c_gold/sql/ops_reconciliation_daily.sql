-- Grain: reference_date × category. Winning-run policy: latest COMPLETED run per date.
CREATE OR REPLACE VIEW gold_ops_reconciliation_daily AS
WITH winning_runs AS (
    SELECT
        reference_date,
        id AS run_id
    FROM (
        SELECT
            reference_date,
            id,
            ROW_NUMBER() OVER (
                PARTITION BY reference_date
                ORDER BY started_at DESC, id DESC
            ) AS rn
        FROM silver_reconciliation_runs
        WHERE status = 'COMPLETED'
    ) ranked
    WHERE rn = 1
),
daily_totals AS (
    SELECT
        wr.reference_date,
        COUNT(*) AS day_total
    FROM silver_reconciliation_results rr
    JOIN winning_runs wr ON rr.run_id = wr.run_id
    GROUP BY wr.reference_date
)
SELECT
    wr.reference_date,
    wr.run_id,
    rr.category,
    COUNT(*)                                       AS txn_count,
    ROUND(COUNT(*) * 1.0 / dt.day_total, 6)       AS pct_of_total,
    SUM(rr.internal_amount)                        AS internal_amount_sum,
    SUM(rr.processor_amount)                       AS processor_amount_sum,
    SUM(rr.difference)                             AS abs_difference_sum
FROM silver_reconciliation_results rr
JOIN winning_runs wr ON rr.run_id = wr.run_id
JOIN daily_totals dt ON wr.reference_date = dt.reference_date
GROUP BY wr.reference_date, wr.run_id, rr.category, dt.day_total
ORDER BY wr.reference_date, rr.category
