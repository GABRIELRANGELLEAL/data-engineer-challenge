.PHONY: help up down build shell logs test generate generate-large clean \
        seed-silver seed-company build-gold run-alerts run-cfo-report
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

generate: ## Generate a small synthetic dataset (10k rows) into /tmp/generated
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 10000 --days 30 --merchants 100 --seed 42 --out /tmp/generated

generate-large: ## Generate a large synthetic dataset (1M rows) into /tmp/generated-large
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 1000000 --days 90 --merchants 500 --seed 42 --out /tmp/generated-large

seed-silver: ## Seed silver reconciliation tables from sample parquets
	$(PIPELINE) python -m src.silver.cdc_reconc_historical_load \
		docs/sample-data/reconciliation_runs.parquet \
		docs/sample-data/reconciliation_results.parquet

seed-company: ## Seed silver_enterprise_company from sample parquet
	$(PIPELINE) python -m src.silver.enterprise_company \
		docs/sample-data/enterprise_company.parquet

build-gold: ## Build gold layer views and tables
	$(PIPELINE) python -m src.gold.build

run-alerts: ## Run ops alert for the latest reconciled date
	$(PIPELINE) python -m src.outputs.ops_alert "" output/reports

run-cfo-report: ## Render CFO weekly report for the latest week
	$(PIPELINE) python -m src.outputs.cfo_report

clean: ## Remove generated artifacts and stop the container
	-$(PIPELINE) rm -rf /tmp/generated /tmp/generated-large
	$(COMPOSE) down
