# Gold Layer — Schema & Design Reference

> **Product context** (who consumes each artifact and why) lives in `docs/produtos-de-dados.md`.
> This document covers the technical decisions behind the schema.

---

## Project structure

```
src/
├── db.py                          # shared DuckDB connection
├── bronze/
│   ├── settlement_loader.py       # loads PaySettler CSV → raw_paysettler_settlements
│   └── cdc_loader.py              # loads transaction parquets → raw_transactions (CDC)
├── silver/
│   ├── cdc_reconc_historical_load.py   # seeds silver_reconciliation_runs / _results
│   ├── enterprise_company.py           # seeds silver_enterprise_company (CDC)
│   └── reconcile.py                    # daily reconciliation run logic
├── gold/
│   ├── build.py                   # thin runner — reads sql/ files and executes them
│   └── sql/
│       ├── ops_reconciliation_daily.sql
│       ├── ops_reconciliation_trend.sql
│       ├── cfo_weekly_summary.sql
│       ├── cfo_weekly_merchant_ranking.sql
│       └── compliance_ledger.sql
└── outputs/
    ├── ops_alert.py               # Slack Block Kit JSON + SVG chart
    └── cfo_report.py              # HTML email report

tests/
├── test_settlement_loader.ipynb
├── test_cdc_loader.ipynb
└── test_gold.ipynb                # gold layer tests (in-memory DuckDB fixture)

docs/
├── domain-glossary.md
├── gold-schema.md                 # this file
├── produtos-de-dados.md           # product rationale per consumer
└── sample-data/
```

---

## Data flow

```
PostgreSQL (settlement_db)
        │
        ▼
  Bronze layer (raw_*)
  ├── raw_transactions
  └── raw_paysettler_settlements
        │
        ▼
  Silver layer (silver_*)
  ├── silver_reconciliation_runs
  ├── silver_reconciliation_results   ← append-only, immutable
  └── silver_enterprise_company
        │
        ▼
  Gold layer (gold_*)
  ├── gold_ops_reconciliation_daily   (VIEW)
  ├── gold_ops_reconciliation_trend   (VIEW)
  ├── gold_cfo_weekly_summary         (TABLE)
  ├── gold_cfo_weekly_merchant_ranking (TABLE)
  └── gold_compliance_ledger          (VIEW)
        │
        ▼
  Outputs
  └── outputs/
        ├── {date}_alert.json + _chart.svg
        └── {start}_{end}_cfo_report.html
```

---

## Silver tables (gold's input)

### `silver_reconciliation_runs`

| Column | Type | Notes |
|--------|------|-------|
| `id` | BIGINT PK | Auto-incremented via sequence |
| `reference_date` | DATE | Business date reconciled |
| `file_name` | VARCHAR | Source CSV name |
| `status` | VARCHAR | `IN_PROGRESS`, `COMPLETED`, `FAILED` |
| `total_transactions` | INTEGER | Set on completion |
| `started_at` | TIMESTAMPTZ | |
| `completed_at` | TIMESTAMPTZ | |
| `created_at` | TIMESTAMPTZ | |
| `source` | VARCHAR | `historical_backfill` or `computed` |

### `silver_reconciliation_results`

| Column | Type | Notes |
|--------|------|-------|
| `id` | BIGINT PK | |
| `run_id` | BIGINT | FK → runs.id |
| `transaction_id` | VARCHAR | |
| `merchant_id` | VARCHAR | |
| `category` | VARCHAR | `MATCHED`, `MISMATCHED`, `UNRECONCILED_PROCESSOR`, `UNRECONCILED_INTERNAL` |
| `internal_amount` | DECIMAL(18,2) | NULL for UNRECONCILED_PROCESSOR |
| `processor_amount` | DECIMAL(18,2) | NULL for UNRECONCILED_INTERNAL |
| `difference` | DECIMAL(18,2) | NULL unless both sides present |
| `created_at` | TIMESTAMPTZ | |
| `source` | VARCHAR | |

Silver is **append-only**: reprocessing a date creates a new run and appends new result rows. Existing rows are never updated or deleted. This is what makes views over silver always correct.

### `silver_enterprise_company`

| Column | Type | Notes |
|--------|------|-------|
| `id` | BIGINT PK | |
| `merchant_id` | VARCHAR | Join key used by gold |
| `legal_name` | VARCHAR | Razão social |
| `trade_name` | VARCHAR | Nome fantasia |
| `document` | VARCHAR | CNPJ |
| `primary_cnae` | VARCHAR | |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | |

---

## Gold artifacts

### `gold_ops_reconciliation_daily` — VIEW

**Grain:** `reference_date × category`

| Column | Type | Notes |
|--------|------|-------|
| `reference_date` | DATE | |
| `run_id` | BIGINT | Winning run ID for that date |
| `category` | VARCHAR | |
| `txn_count` | BIGINT | |
| `pct_of_total` | DOUBLE | Share of the day's total transactions |
| `internal_amount_sum` | DECIMAL | Sum of internal amounts for this category |
| `processor_amount_sum` | DECIMAL | Sum of processor amounts |
| `abs_difference_sum` | DECIMAL | Sum of absolute differences (NULL for unreconciled) |

Winning-run policy applied (see below). Always current — views over immutable silver are never stale.

---

### `gold_ops_reconciliation_trend` — VIEW

**Grain:** `reference_date × category`

Extends `gold_ops_reconciliation_daily` with one additional column:

| Column | Type | Notes |
|--------|------|-------|
| `pct_of_total_7d_avg` | DOUBLE | 7-day trailing avg of `pct_of_total`, **excluding current row** (`ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING`) |

Used by `ops_alert.py` to decide whether today's rate is out of pattern.

---

### `gold_cfo_weekly_summary` — TABLE

**Grain:** `week_start × category`

| Column | Type | Notes |
|--------|------|-------|
| `week_start` | DATE | Monday of the ISO week |
| `week_end` | DATE | Sunday of the ISO week |
| `category` | VARCHAR | |
| `txn_count` | BIGINT | |
| `amount_brl` | DECIMAL | `COALESCE(processor_amount, internal_amount)` — see convention below |

Materialized as a table so the weekly report is an **auditable frozen snapshot** that does not change if past `reference_date`s are reprocessed later. Winning-run policy applied per `reference_date` before aggregating to week.

---

### `gold_cfo_weekly_merchant_ranking` — TABLE

**Grain:** `week_start × merchant_id × category`

| Column | Type | Notes |
|--------|------|-------|
| `week_start` | DATE | |
| `merchant_id` | VARCHAR | |
| `category` | VARCHAR | Non-matched only (MISMATCHED, UNRECONCILED_*) |
| `txn_count` | BIGINT | |
| `amount_brl` | DECIMAL | COALESCE convention |
| `legal_name` | VARCHAR | From `silver_enterprise_company` (NULL if no record) |
| `trade_name` | VARCHAR | From `silver_enterprise_company` |

No `LIMIT` — the report script selects its own top-N so this table stays reusable for any N.
Only non-matched categories are included: matched transactions have no risk relevance for the CFO.

---

### `gold_compliance_ledger` — VIEW

**Grain:** `result_id` (one row per transaction × run — no aggregation)

| Column | Type | Source |
|--------|------|--------|
| `result_id` | BIGINT | `silver_reconciliation_results.id` |
| `run_id` | BIGINT | |
| `reference_date` | DATE | From runs |
| `file_name` | VARCHAR | From runs |
| `started_at` | TIMESTAMPTZ | From runs |
| `completed_at` | TIMESTAMPTZ | From runs |
| `run_status` | VARCHAR | From runs |
| `transaction_id` | VARCHAR | |
| `merchant_id` | VARCHAR | |
| `legal_name` | VARCHAR | From enterprise_company |
| `document` | VARCHAR | CNPJ from enterprise_company |
| `category` | VARCHAR | |
| `internal_amount` | DECIMAL(18,2) | |
| `processor_amount` | DECIMAL(18,2) | |
| `difference` | DECIMAL(18,2) | |
| `created_at` | TIMESTAMPTZ | |

---

## Design decisions

### Winning-run policy

Silver is append-only: reprocessing a date adds a new run rather than overwriting. Downstream
consumers that show "current state" must pick one run per `reference_date`. The policy:

> **Latest COMPLETED run**, ordered by `started_at DESC, id DESC` (ID breaks ties).

Applied via a `ROW_NUMBER()` window CTE inside each SQL file:

```sql
WITH winning_runs AS (
    SELECT reference_date, id AS run_id
    FROM (
        SELECT reference_date, id,
               ROW_NUMBER() OVER (
                   PARTITION BY reference_date
                   ORDER BY started_at DESC, id DESC
               ) AS rn
        FROM silver_reconciliation_runs
        WHERE status = 'COMPLETED'
    ) ranked
    WHERE rn = 1
)
```

**Compliance deliberately opts out.** Auditors need to see every run — including superseded ones —
to answer questions like "what did the pipeline report at 08:00 before the rerun at 09:00?"
Adding a winning-run filter to `gold_compliance_ledger` would be a silent correctness bug.
`tests/test_gold.ipynb` has a dedicated regression guard for this.

### Why CFO is a materialized table, Ops and Compliance are views

| Artifact | Type | Reason |
|----------|------|--------|
| `ops_reconciliation_daily` | VIEW | Ops needs real-time current state; silver immutability makes views always correct |
| `ops_reconciliation_trend` | VIEW | Same — depends on daily view |
| `cfo_weekly_summary` | TABLE | Weekly report must be a frozen snapshot; CFO should not see numbers change retroactively if a past date is reprocessed |
| `cfo_weekly_merchant_ranking` | TABLE | Same frozen-snapshot requirement |
| `compliance_ledger` | VIEW | Compliance needs the full live history including new runs added after any given query |

### CFO amount convention

```sql
COALESCE(processor_amount, internal_amount) AS amount_brl
```

- **MATCHED / MISMATCHED:** processor amount is present → used as the authoritative value (PaySettler-confirmed).
- **UNRECONCILED_INTERNAL:** no processor amount exists by definition → falls back to internal amount.
- **UNRECONCILED_PROCESSOR:** no internal amount exists → processor amount is used.

This means `amount_brl` is never NULL for any category, and uses the most externally-confirmed value available.

### Closed-week definition (Mon–Sun)

DuckDB's `DATE_TRUNC('week', date)` returns the Monday of the ISO week, which gives a clean
Monday-open / Sunday-close boundary. This enables:
- Stable `week_start` keys (no floating window)
- Consistent "vs. previous week" comparison in the CFO report (`week_start - INTERVAL '7 days'`)

The alternative (rolling 7-day window) would make the comparison headline non-deterministic
and harder to explain to a non-technical stakeholder.

### Idempotency strategy

Views (`CREATE OR REPLACE VIEW`) are trivially idempotent.

Tables (`CREATE OR REPLACE TABLE`) drop and recreate on each `build()` call. This is
appropriate for local and development use. **Known limit:** in production, a true weekly
freeze would require an explicit "close week" step that marks rows immutable and prevents
`build()` from overwriting historical snapshots. A simple approach would be to partition
by `week_start` and only rebuild the current (open) week, leaving prior weeks untouched.

---

## Running the gold layer

```bash
# 1. Seed silver (if not already done)
python -m src.b_silver.cdc_reconc \
    docs/sample-data/reconciliation_runs.parquet \
    docs/sample-data/reconciliation_results.parquet

python -m src.b_silver.cdc_company \
    docs/sample-data/enterprise_company.parquet

# 2. Build gold
python -m src.c_gold.build

# 3. Run outputs
python -m src.products.ops_alert             # defaults to latest reference_date
python -m src.products.cfo_report            # aggregates the entire available period

# Or via make
make build-gold
make run-alerts
make run-cfo-report
```
