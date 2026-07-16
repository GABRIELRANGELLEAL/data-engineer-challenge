-- Grain: week_start × merchant_id × category. Materialized table.
-- Risk-focused: only non-matched categories (MISMATCHED, UNRECONCILED_*).
-- No LIMIT here — the report script selects its own top-N so this table stays reusable.
-- Enriched with legal_name, trade_name from silver_enterprise_company (LEFT JOIN — some
-- merchant_ids may not yet have company records).
CREATE OR REPLACE TABLE gold_cfo_weekly_merchant_ranking AS
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
)
SELECT
    DATE_TRUNC('week', wr.reference_date)::DATE                        AS week_start,
    rr.merchant_id,
    rr.category,
    COUNT(*)                                                            AS txn_count,
    SUM(COALESCE(rr.processor_amount, rr.internal_amount))              AS amount_brl,
    ec.legal_name,
    ec.trade_name
FROM silver_reconciliation_results rr
JOIN winning_runs wr ON rr.run_id = wr.run_id
LEFT JOIN silver_enterprise_company ec ON rr.merchant_id = ec.merchant_id
WHERE rr.category IN ('MISMATCHED', 'UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')
GROUP BY
    DATE_TRUNC('week', wr.reference_date)::DATE,
    rr.merchant_id,
    rr.category,
    ec.legal_name,
    ec.trade_name
ORDER BY week_start, amount_brl DESC
