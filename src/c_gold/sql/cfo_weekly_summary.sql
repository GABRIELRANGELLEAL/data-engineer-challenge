-- Grain: week_start × category. Materialized table — frozen weekly snapshot for audit.
-- Week definition: closed Mon–Sun (ISO week). Winning-run policy applied per reference_date.
-- amount_brl: COALESCE(processor_amount, internal_amount) — uses PaySettler-confirmed amount
-- when available; falls back to internal_amount for UNRECONCILED_INTERNAL rows only.
CREATE OR REPLACE TABLE gold_cfo_weekly_summary AS
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
weekly AS (
    SELECT
        DATE_TRUNC('week', wr.reference_date)::DATE            AS week_start,
        rr.category,
        COUNT(*)                                                AS txn_count,
        SUM(COALESCE(rr.processor_amount, rr.internal_amount)) AS amount_brl
    FROM silver_reconciliation_results rr
    JOIN winning_runs wr ON rr.run_id = wr.run_id
    GROUP BY
        DATE_TRUNC('week', wr.reference_date)::DATE,
        rr.category
)
SELECT
    week_start,
    (week_start + INTERVAL '6 days')::DATE AS week_end,
    category,
    txn_count,
    amount_brl
FROM weekly
ORDER BY week_start, category