-- Grain: reference_date. The truly most recent run attempt for each date,
-- whatever its status (no COMPLETED filter) — lets the gold layer surface
-- run_status even when the latest attempt failed and produced no results.
CREATE OR REPLACE VIEW silver_reconciliation_runs_latest AS
SELECT * EXCLUDE (_rn)
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY reference_date
            ORDER BY started_at DESC, id DESC
        ) AS _rn
    FROM silver_reconciliation_runs
)
WHERE _rn = 1
