.PHONY: help up down build shell logs test test-generate-sample-data generate generate-large clean \
        load-cdc-transactions load-settlement load-reconciliation-runs load-reconciliation-results \
        load-enterprise-company seed-silver seed-company build-silver build-gold \
        run-alerts run-cfo-report run-ops-run-report run_pipeline
.DEFAULT_GOAL := help

COMPOSE := docker compose
PIPELINE := $(COMPOSE) exec pipeline

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Build and start the container in the background
	$(COMPOSE) up -d --build

down: ## Stop and remove the container
	$(COMPOSE) down

build: ## Rebuild the image without starting the container
	$(COMPOSE) build

shell: ## Open an interactive shell inside the container
	$(PIPELINE) bash

logs: ## Tail container logs
	$(COMPOSE) logs -f

test: ## Run the generator smoke tests inside the container
	$(PIPELINE) pytest tests/ -v

test-generate-sample-data: ## Run only the sample-data generator smoke tests inside the container
	$(PIPELINE) pytest tests/test_generate_sample_data.py -v

generate: ## Generate a small synthetic dataset (10k rows) into docs/sample-data
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 10000 --days 30 --merchants 100 --seed 42 --out docs/sample-data

generate-large: ## Generate a large synthetic dataset (1M rows) into docs/sample-data
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 1000000 --days 90 --merchants 500 --seed 42 --out docs/sample-data

load-cdc-transactions: ## Bronze: load internal transaction batches (CDC) into raw_transactions
	$(PIPELINE) python -c "from src.a_bronze.cdc_transaction import load_transactions; load_transactions(['docs/sample-data/transactions_batch_1.parquet', 'docs/sample-data/transactions_batch_2.parquet'])"
	$(PIPELINE) python scripts/print_health.py raw_transactions --pk transaction_id

load-settlement: ## Bronze: load all PaySettler CSVs from docs/sample-data/paysettler into raw_paysettler_settlements
	$(PIPELINE) python -c "from src.a_bronze.settlement_loader import load_directory; from src.db import get_connection; conn = get_connection(); results = load_directory('docs/sample-data/paysettler', r'(\d{4}-\d{2}-\d{2})', conn=conn); total = conn.execute('SELECT COUNT(*) FROM raw_paysettler_settlements').fetchone()[0]; print(f'Arquivos processados: {len(results)}'); print(f'Total de linhas em raw_paysettler_settlements: {total}')"
	$(PIPELINE) python scripts/print_health.py raw_paysettler_settlements --pk transaction_id,reference_date

load-reconciliation-runs: ## Bronze: load reconciliation_runs.parquet into raw_reconciliation_runs
	$(PIPELINE) python -m src.a_bronze.reconciliation_runs docs/sample-data/reconciliation_runs.parquet
	$(PIPELINE) python scripts/print_health.py raw_reconciliation_runs --pk id

load-reconciliation-results: ## Bronze: load reconciliation_results.parquet into raw_reconciliation_results
	$(PIPELINE) python -m src.a_bronze.reconciliation_results docs/sample-data/reconciliation_results.parquet
	$(PIPELINE) python scripts/print_health.py raw_reconciliation_results --pk id

load-enterprise-company: ## Bronze: load enterprise_company.parquet into raw_enterprise_company
	$(PIPELINE) python -m src.a_bronze.enterprise_company docs/sample-data/enterprise_company.parquet
	$(PIPELINE) python scripts/print_health.py raw_enterprise_company --pk id

seed-silver: load-reconciliation-runs load-reconciliation-results ## Silver: (re)build reconciliation tables from bronze raw extracts (CREATE OR REPLACE, idempotent)
	$(PIPELINE) python -m src.b_silver.cdc_reconc
	$(PIPELINE) python scripts/print_health.py silver_reconciliation_runs --pk id
	$(PIPELINE) python scripts/print_health.py silver_reconciliation_results --pk id

seed-company: load-enterprise-company ## Silver: (re)build silver_enterprise_company from bronze raw extract (CREATE OR REPLACE, idempotent)
	$(PIPELINE) python -m src.b_silver.cdc_company
	$(PIPELINE) python scripts/print_health.py silver_enterprise_company --pk id

build-silver: seed-silver seed-company ## Silver: build curated views (winning-run + enrichment)
	$(PIPELINE) python -m src.b_silver.build

build-gold: build-silver ## Gold: build gold layer views and tables
	$(PIPELINE) python -m src.c_gold.build
	$(PIPELINE) python scripts/print_health.py gold_ops_reconciliation_daily --pk reference_date,category
	$(PIPELINE) python scripts/print_health.py gold_ops_reconciliation_trend --pk reference_date,category
	$(PIPELINE) python scripts/print_health.py gold_cfo_weekly_summary --pk week_start,category
	$(PIPELINE) python scripts/print_health.py gold_cfo_weekly_merchant_ranking --pk week_start,merchant_id,category
	$(PIPELINE) python scripts/print_health.py gold_compliance_ledger --pk result_id
	$(PIPELINE) python scripts/print_health.py gold_ops_run_history --pk run_id,category

run-alerts: ## Run ops alert for the latest reconciled date
	$(PIPELINE) python -m src.products.ops_alert "" outputs

run-cfo-report: ## Render CFO report for the entire available period
	$(PIPELINE) python -m src.products.cfo_report

run-ops-run-report: ## Render ops run-history report (last 8 days, all run attempts)
	$(PIPELINE) python -m src.products.ops_run_report "" outputs

run_pipeline: generate build-gold run-alerts run-cfo-report run-ops-run-report ## Run the full pipeline end-to-end: generate sample data, build gold, render all products

clean: ## Remove generated artifacts and stop the container
	-$(PIPELINE) rm -rf docs/sample-data/*
	$(COMPOSE) down
