-- Grain: transaction × winning run. Winning-run policy: latest COMPLETED run per
-- reference_date (status filtered BEFORE ranking, so a FAILED latest attempt does not
-- hide an earlier COMPLETED run's results for that date). Enriched with merchant
-- identity columns so gold artifacts don't need to repeat the silver_enterprise_company
-- join themselves.
CREATE OR REPLACE VIEW silver_reconciliation_results_current AS
WITH winning_runs AS (
    SELECT reference_date, id AS run_id
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
    rr.id,
    rr.run_id,
    wr.reference_date,
    rr.transaction_id,
    rr.merchant_id,
    rr.category,
    rr.internal_amount,
    rr.processor_amount,
    rr.difference,
    rr.created_at,
    rr.source,
    ec.legal_name,
    ec.trade_name,
    ec.document,
    ec.primary_cnae
FROM silver_reconciliation_results rr
JOIN winning_runs wr ON rr.run_id = wr.run_id
LEFT JOIN silver_enterprise_company ec ON rr.merchant_id = ec.merchant_id
