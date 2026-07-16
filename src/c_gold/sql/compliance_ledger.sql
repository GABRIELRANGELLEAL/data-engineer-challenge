-- Grain: transaction × run. No aggregation, no winning-run filter.
-- Deliberately shows ALL runs including superseded ones — auditability requires the full history.
-- Joins runs (for reference_date, file_name, timing) and enterprise_company (for legal identity).
CREATE OR REPLACE VIEW gold_compliance_ledger AS
SELECT
    rr.id              AS result_id,
    rr.run_id,
    run.reference_date,
    run.file_name,
    run.started_at,
    run.completed_at,
    run.status         AS run_status,
    rr.transaction_id,
    rr.merchant_id,
    ec.legal_name,
    ec.document,
    rr.category,
    rr.internal_amount,
    rr.processor_amount,
    rr.difference,
    rr.created_at
FROM silver_reconciliation_results rr
JOIN silver_reconciliation_runs run ON rr.run_id = run.id
LEFT JOIN silver_enterprise_company ec ON rr.merchant_id = ec.merchant_id
