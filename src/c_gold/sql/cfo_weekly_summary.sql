-- Grain: week_start × category. Materialized table — frozen weekly snapshot for audit.
-- Week definition: closed Mon–Sun (ISO week). Sourced from
-- silver_reconciliation_results_current, which already applies the winning-run
-- policy (latest COMPLETED run per reference_date).
-- amount_brl: COALESCE(processor_amount, internal_amount) — uses PaySettler-confirmed amount
-- when available; falls back to internal_amount for UNRECONCILED_INTERNAL rows only.
CREATE OR REPLACE TABLE gold_cfo_weekly_summary AS
WITH weekly AS (
    SELECT
        DATE_TRUNC('week', reference_date)::DATE                AS week_start,
        category,
        COUNT(*)                                                 AS txn_count,
        SUM(COALESCE(processor_amount, internal_amount))        AS amount_brl
    FROM silver_reconciliation_results_current
    GROUP BY
        DATE_TRUNC('week', reference_date)::DATE,
        category
)
SELECT
    week_start,
    (week_start + INTERVAL '6 days')::DATE AS week_end,
    category,
    txn_count,
    amount_brl
FROM weekly
ORDER BY week_start, category
