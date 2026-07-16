-- Grain: week_start × merchant_id × category. Materialized table.
-- Risk-focused: only non-matched categories (MISMATCHED, UNRECONCILED_*).
-- No LIMIT here — the report script selects its own top-N so this table stays reusable.
-- Sourced from silver_reconciliation_results_current, which already applies the
-- winning-run policy and the LEFT JOIN to silver_enterprise_company for
-- legal_name/trade_name (some merchant_ids may not yet have company records).
CREATE OR REPLACE TABLE gold_cfo_weekly_merchant_ranking AS
SELECT
    DATE_TRUNC('week', reference_date)::DATE                AS week_start,
    merchant_id,
    category,
    COUNT(*)                                                 AS txn_count,
    SUM(COALESCE(processor_amount, internal_amount))        AS amount_brl,
    legal_name,
    trade_name
FROM silver_reconciliation_results_current
WHERE category IN ('MISMATCHED', 'UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')
GROUP BY
    DATE_TRUNC('week', reference_date)::DATE,
    merchant_id,
    category,
    legal_name,
    trade_name
ORDER BY week_start, amount_brl DESC
