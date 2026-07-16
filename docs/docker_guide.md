```bash
cd /mnt/c/Users/Leal/data-engineer-challenge
```

### Sobe o container (build da imagem + inicia em idle)
```bash
docker compose up -d --build
```

### Passo a passo manual (mesmas etapas do pipeline, uma por uma)

#### 1. Gerar dados de amostra
```bash
make generate                    # 10k linhas em docs/sample-data
make generate-large              # 1M linhas em docs/sample-data
```

#### 2. Bronze — carga bruta (dedup, CDC, normalização)

**CDC transactions** (`src/a_bronze/cdc_transaction.py` — une os batches de transações internas e aplica CDC para o snapshot atual em `raw_transactions`):
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

#### 3. Silver — camada curada (append-only, quality gates, audit trail)

**CDC reconc** (`src/b_silver/cdc_reconc.py` — seed histórico de `silver_reconciliation_runs`/`silver_reconciliation_results` a partir dos parquets de exemplo):
```bash
make seed-silver
# equivalente manual:
docker compose exec pipeline python -c "
from src.b_silver.cdc_reconc import seed
seed('docs/sample-data/reconciliation_runs.parquet', 'docs/sample-data/reconciliation_results.parquet')
"
```

Reconciliação incremental para uma data específica (`src/b_silver/reconcile.py`, após rodar o bronze do passo 2):
```bash
docker compose exec pipeline python -c "
from src.b_silver.reconcile import reconcile
reconcile('2025-03-15')
"
```

**CDC company** (`src/b_silver/cdc_company.py` — dados cadastrais dos merchants → `silver_enterprise_company`):
```bash
make seed-company
# equivalente manual:
docker compose exec pipeline python -m src.b_silver.cdc_company \
    docs/sample-data/enterprise_company.parquet
```

#### 4. Gold — views e tabelas analíticas por consumidor
```bash
make build-gold
# equivalente manual:
docker compose exec pipeline python -m src.c_gold.build
```

#### 5. Produtos de dados — outputs finais
```bash
make run-alerts        # outputs/{date}_alert.json + _chart.svg (Ops)
make run-cfo-report    # outputs/{start}_{end}_cfo_report.html (CFO)
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
