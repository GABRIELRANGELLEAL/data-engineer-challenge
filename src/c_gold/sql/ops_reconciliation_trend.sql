-- View on gold_ops_reconciliation_daily. Adds 7-day trailing moving average of pct_of_total
-- per category (7 preceding rows, excluding current row). Used by the Ops alert to detect
-- whether today's rate is out of pattern relative to recent history.
CREATE OR REPLACE VIEW gold_ops_reconciliation_trend AS
SELECT
    reference_date,
    run_id,
    category,
    txn_count,
    pct_of_total,
    internal_amount_sum,
    processor_amount_sum,
    abs_difference_sum,
    AVG(pct_of_total) OVER (
        PARTITION BY category
        ORDER BY reference_date
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS pct_of_total_7d_avg
FROM gold_ops_reconciliation_daily
