```bash
cd /mnt/c/Users/Leal/data-engineer-challenge
```

### Sobe o container (build da imagem + inicia em idle)
```bash
docker compose up -d --build
```

### Roda o pipeline
```bash
make run_pipeline
```

### Passo a passo manual (mesmas etapas do pipeline, uma por uma)

#### 1. Gerar dados de amostra
```bash
make generate           # 10k linhas em docs/sample-data
make generate-large     # 1M linhas em docs/sample-data
```

#### 2. Bronze — landing bruto (sem regra de negócio)

**CDC transactions** (`src/a_bronze/cdc_transaction.py` — une os batches de transações internas e já aplica CDC para o snapshot atual em `raw_transactions`; esta é a única tabela bronze que sai deduplicada — as outras três abaixo fazem landing puro):
```bash
make load-cdc-transactions
# equivalente manual:
docker compose exec pipeline python -c "
from src.a_bronze.cdc_transaction import load_transactions
load_transactions([
    'docs/sample-data/transactions_batch_1.parquet',
    'docs/sample-data/transactions_batch_2.parquet',
])
"
```
**Reconciliation runs / results / enterprise company** (`src/a_bronze/reconciliation_runs.py`, `reconciliation_results.py`, `enterprise_company.py` — landing puro do parquet, sem CDC; a dedup fica por conta da silver no passo 3):
```bash
make load-reconciliation-runs
make load-reconciliation-results
make load-enterprise-company
# equivalente manual (mesmo padrão para os três):
docker compose exec pipeline python -m src.a_bronze.reconciliation_runs docs/sample-data/reconciliation_runs.parquet
docker compose exec pipeline python -m src.a_bronze.reconciliation_results docs/sample-data/reconciliation_results.parquet
docker compose exec pipeline python -m src.a_bronze.enterprise_company docs/sample-data/enterprise_company.parquet
```

**Settlement loader (PaySettler CSV)** (`src/a_bronze/settlement_loader.py` — carrega todos os CSVs diários da pasta em `raw_paysettler_settlements`; imprime quantos arquivos foram processados e o total de linhas na tabela):
```bash
make load-settlement
# saída esperada:
#   Arquivos processados: 44
#   Total de linhas em raw_paysettler_settlements: <N>

# equivalente manual:
docker compose exec pipeline python -c "
from src.a_bronze.settlement_loader import load_directory
from src.db import get_connection

conn = get_connection()
results = load_directory('docs/sample-data/paysettler', r'(\d{4}-\d{2}-\d{2})', conn=conn)
total = conn.execute('SELECT COUNT(*) FROM raw_paysettler_settlements').fetchone()[0]
print(f'Arquivos processados: {len(results)}')
print(f'Total de linhas em raw_paysettler_settlements: {total}')
"
```


#### 3. Silver — CDC dedup + views curadas

**CDC reconc** (`src/b_silver/cdc_reconc.py` — (re)constrói `silver_reconciliation_runs`/`silver_reconciliation_results` do zero a partir de `raw_reconciliation_runs`/`raw_reconciliation_results`, aplicando CDC dedup; `CREATE OR REPLACE TABLE`, idempotente — pode rodar quantas vezes quiser):
```bash
make seed-silver        # já roda os load-reconciliation-* da bronze como pré-requisito
# equivalente manual:
docker compose exec pipeline python -m src.b_silver.cdc_reconc
```

**CDC company** (`src/b_silver/cdc_company.py` — (re)constrói `silver_enterprise_company` do zero a partir de `raw_enterprise_company`; `CREATE OR REPLACE TABLE`, idempotente):
```bash
make seed-company       # já roda o load-enterprise-company da bronze como pré-requisito
# equivalente manual:
docker compose exec pipeline python -m src.b_silver.cdc_company
```

**Views curadas** (`src/b_silver/build.py` — constrói `silver_reconciliation_runs_latest` e `silver_reconciliation_results_current`: filtram pelo winning-run e enriquecem com dados do merchant, centralizando essa lógica para a gold layer não precisar repeti-la):
```bash
make build-silver        # já roda seed-silver + seed-company como pré-requisito
# equivalente manual:
docker compose exec pipeline python -m src.b_silver.build
```

#### 4. Gold — views e tabelas analíticas por consumidor
```bash
make build-gold           # roda build-silver como pré-requisito — a cadeia inteira é idempotente,
                           # pode rodar quantas vezes quiser
# equivalente manual:
docker compose exec pipeline python -m src.c_gold.build
```

#### 5. Produtos de dados — outputs finais
```bash
make run-alerts           # outputs/{date}_alert.json + _chart.svg (Ops)
make run-ops-run-report   # outputs/{date}_ops_run_report.html — todas as tentativas de run
                           # (falhadas ou não) dos últimos 8 dias (data + 7 dias antes),
                           # via gold_ops_run_history (Ops)
make run-cfo-report        # outputs/{start}_{end}_cfo_report.html (CFO)
# Compliance não tem script — gold_compliance_ledger é consultado via SQL direto
```

#### 6. (Opcional) Testes
```bash
make test                        # toda a suíte
make test-generate-sample-data   # só os testes do gerador de dados
```

O banco DuckDB persiste em `data/warehouse.duckdb`. Para inspecionar diretamente:
```bash
docker compose exec pipeline python -c "
import duckdb; conn = duckdb.connect('data/warehouse.duckdb')
print(conn.execute('SHOW TABLES').fetchdf())
"
```

Stop the container
```bash
docker compose down
```