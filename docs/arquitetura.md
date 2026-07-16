# Arquitetura - Pipeline de Reconciliacao de Liquidacoes

## Visao Geral

Pipeline medalhao (Bronze / Silver / Gold) em DuckDB local, orquestrado via Makefile, containerizado com Docker Compose.

```
  Fontes Externas                        Produtos de Dados
  ─────────────────                      ─────────────────
  Parquet (CDC)   ──┐                ┌── Alerta Ops (JSON + SVG)
  CSV PaySettler  ──┼── Bronze ── Silver ── Gold ──┼── Relatorio CFO (HTML)
  Parquet (seed)  ──┘                └── Relatorio Run History (HTML)
```

Para decisoes de design, produtos de dados, como rodar e observabilidade, veja o [readme_solution.md](../readme_solution.md).

---

## Bronze Layer

Ingestao de dados brutos no DuckDB com transformacoes minimas.

### `raw_transactions`

Snapshot atual das transacoes internas, derivado dos batches Parquet com CDC aplicado (dedup por `_timestamp DESC`, exclui `Op='D'`). Schema drift entre batches e tratado via `union_by_name` — colunas ausentes em um batch recebem NULL.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | bigint | PK original do sistema fonte |
| `transaction_id` | varchar | UUID da transacao |
| `merchant_id` | varchar | Identificador do merchant |
| `amount` | decimal | Valor da transacao (BRL) |
| `currency` | varchar | Moeda (ISO 4217) |
| `status` | varchar | `COMPLETED`, `PENDING`, `FAILED` |
| `description` | varchar | Descricao da transacao |
| `payment_method` | varchar | Metodo de pagamento (NULL em batch_1 — schema drift) |
| `created_at` | timestamp | Data de criacao |
| `updated_at` | timestamp | Ultima atualizacao |
| `Op` | varchar | Operacao CDC: `I` (insert), `U` (update), `D` (delete) |
| `_timestamp` | timestamp | Timestamp do evento CDC |

---

### `raw_paysettler_settlements`

Liquidacoes do processador externo PaySettler, carregadas a partir de CSVs diarios.

> **Nota sobre o contexto do case:** neste desafio, os dados do PaySettler sao mocados — os CSVs em `docs/sample-data/paysettler/` sao gerados sinteticamente. O `settlement_loader.py` existe para demonstrar como o pipeline processaria CSVs reais em producao: validacao de schema, normalizacao de amounts BRL (detecta e converte tanto `152.30` quanto `R$ 32.245,91`), dedup intra-arquivo, quality gates que rejeitam linhas com amounts zero/negativos ou IDs vazios, e carga idempotente via `INSERT OR REPLACE` na PK composta. Em producao, esses CSVs chegariam via SFTP ou S3 e o loader seria acionado por um sensor do orquestrador.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `transaction_id` | varchar | PK (composta com `reference_date`). UUID da transacao |
| `reference_date` | date | PK. Data de negocio que o CSV representa |
| `merchant_id` | varchar | Identificador do merchant |
| `amount` | decimal(18,2) | Valor liquidado, normalizado para formato numerico |
| `currency` | varchar(3) | Moeda (ISO 4217) |
| `settled_at` | timestamptz | Data/hora da liquidacao (UTC) |
| `processor_reference` | varchar | Referencia interna do PaySettler |
| `status` | varchar | `SETTLED` ou `REVERSED` (check constraint) |
| `_loaded_at` | timestamp | Timestamp de carga (metadata) |
| `_source_file` | varchar | Nome do arquivo CSV de origem (metadata) |

**Quality gates:**
- Arquivos com colunas faltantes: rejeitados antes da carga
- Arquivos vazios (0 data rows): rejeitados
- Amounts zero ou negativos: halt (ValueError)
- `transaction_id` ou `merchant_id` NULL/vazio: halt
- Duplicatas intra-arquivo no `transaction_id`: warning + last occurrence wins

---

### `raw_reconciliation_runs`

Landing puro do Parquet de execucoes de reconciliacao. `CREATE OR REPLACE TABLE` — sem transformacao.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | bigint | PK |
| `reference_date` | varchar | Data de referencia (cast para DATE na silver) |
| `file_name` | varchar | Nome do arquivo processado |
| `status` | varchar | `IN_PROGRESS`, `COMPLETED`, `FAILED` |
| `total_transactions` | varchar | Total de transacoes no arquivo (cast para INTEGER na silver) |
| `started_at` | varchar | Inicio do processamento (cast para TIMESTAMPTZ na silver) |
| `completed_at` | varchar | Fim do processamento |
| `created_at` | varchar | Data de criacao do registro |
| `Op` | varchar | Operacao CDC |
| `_timestamp` | varchar | Timestamp do evento CDC |

---

### `raw_reconciliation_results`

Landing puro do Parquet de resultados de reconciliacao. `CREATE OR REPLACE TABLE` — sem transformacao.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | bigint | PK |
| `run_id` | bigint | FK → `reconciliation_runs.id` |
| `transaction_id` | varchar | UUID da transacao |
| `merchant_id` | varchar | Identificador do merchant |
| `category` | varchar | `MATCHED`, `MISMATCHED`, `UNRECONCILED_PROCESSOR`, `UNRECONCILED_INTERNAL` |
| `internal_amount` | varchar | Valor no sistema interno (cast para DECIMAL na silver) |
| `processor_amount` | varchar | Valor no PaySettler |
| `difference` | varchar | Diferenca absoluta |
| `created_at` | varchar | Data de criacao |
| `Op` | varchar | Operacao CDC |
| `_timestamp` | varchar | Timestamp do evento CDC |

---

### `raw_enterprise_company`

Landing puro do Parquet de dados cadastrais dos merchants. `CREATE OR REPLACE TABLE`.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | bigint | PK |
| `merchant_id` | varchar | Codigo do merchant |
| `legal_name` | varchar | Razao social |
| `trade_name` | varchar | Nome fantasia |
| `document` | varchar | CNPJ |
| `primary_cnae` | varchar | CNAE principal |
| `created_at` | varchar | Data de criacao |
| `updated_at` | varchar | Ultima atualizacao |
| `Op` | varchar | Operacao CDC |
| `_timestamp` | varchar | Timestamp do evento CDC |

---

## Silver Layer

CDC deduplicado + views curadas com regras de negocio.

### `silver_reconciliation_runs` (tabela)

Dedup de `raw_reconciliation_runs`: `ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC)`, exclui `Op='D'`. Tipos castados para os tipos corretos.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | bigint | PK |
| `reference_date` | date | Data de referencia do run |
| `file_name` | varchar | Nome do arquivo processado |
| `status` | varchar | `IN_PROGRESS`, `COMPLETED`, `FAILED` |
| `total_transactions` | integer | Total de transacoes no arquivo |
| `started_at` | timestamptz | Inicio do processamento |
| `completed_at` | timestamptz | Fim do processamento |
| `created_at` | timestamptz | Data de criacao do registro |
| `source` | varchar | Origem: `historical_backfill` (seed) ou identificador do run |

---

### `silver_reconciliation_results` (tabela)

Dedup de `raw_reconciliation_results` com mesma logica CDC. Tipos castados.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | bigint | PK |
| `run_id` | bigint | FK → `silver_reconciliation_runs.id` |
| `transaction_id` | varchar | UUID da transacao |
| `merchant_id` | varchar | Identificador do merchant |
| `category` | varchar | Categoria de reconciliacao |
| `internal_amount` | decimal(18,2) | Valor no sistema interno |
| `processor_amount` | decimal(18,2) | Valor no PaySettler |
| `difference` | decimal(18,2) | Diferenca absoluta |
| `created_at` | timestamptz | Data de criacao |
| `source` | varchar | Origem do registro |

---

### `silver_enterprise_company` (tabela)

Dedup de `raw_enterprise_company` com mesma logica CDC.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | bigint | PK |
| `merchant_id` | varchar | Codigo do merchant |
| `legal_name` | varchar | Razao social |
| `trade_name` | varchar | Nome fantasia |
| `document` | varchar | CNPJ |
| `primary_cnae` | varchar | CNAE principal |
| `created_at` | timestamptz | Data de criacao |
| `updated_at` | timestamptz | Ultima atualizacao |

---

### `silver_reconciliation_runs_latest` (view)

Grao: `reference_date`. O run **mais recente** para cada data, independente de status. Usado pelo gold para surfacear falhas — se o run mais recente falhou, essa view ainda o retorna (diferente da winning-run policy que exige COMPLETED).

```sql
ROW_NUMBER() OVER (PARTITION BY reference_date ORDER BY started_at DESC, id DESC)
-- Sem filtro de status
```

Colunas: todas de `silver_reconciliation_runs`.

---

### `silver_reconciliation_results_current` (view)

Grao: `transaction x winning run`. Aplica a **winning-run policy**: o run mais recente com `status = 'COMPLETED'` para cada `reference_date`. Enriquecido com dados cadastrais do merchant via LEFT JOIN com `silver_enterprise_company`.

```sql
-- Winning run: latest COMPLETED per reference_date
-- Um run FAILED nao esconde um COMPLETED anterior
ROW_NUMBER() OVER (PARTITION BY reference_date ORDER BY started_at DESC, id DESC)
WHERE status = 'COMPLETED'
```

| Coluna | Tipo | Origem |
|--------|------|--------|
| `id` | bigint | `silver_reconciliation_results.id` |
| `run_id` | bigint | `silver_reconciliation_results.run_id` |
| `reference_date` | date | `winning_runs` (CTE) |
| `transaction_id` | varchar | `silver_reconciliation_results` |
| `merchant_id` | varchar | `silver_reconciliation_results` |
| `category` | varchar | `silver_reconciliation_results` |
| `internal_amount` | decimal(18,2) | `silver_reconciliation_results` |
| `processor_amount` | decimal(18,2) | `silver_reconciliation_results` |
| `difference` | decimal(18,2) | `silver_reconciliation_results` |
| `created_at` | timestamptz | `silver_reconciliation_results` |
| `source` | varchar | `silver_reconciliation_results` |
| `legal_name` | varchar | `silver_enterprise_company` (LEFT JOIN) |
| `trade_name` | varchar | `silver_enterprise_company` (LEFT JOIN) |
| `document` | varchar | `silver_enterprise_company` (LEFT JOIN) |
| `primary_cnae` | varchar | `silver_enterprise_company` (LEFT JOIN) |

---

## Gold Layer

Artefatos analiticos por consumidor, construidos sobre as views curadas da silver.

### `gold_ops_reconciliation_daily` (view)

Grao: `reference_date x category`. Sempre tem uma linha por `reference_date` presente em `silver_reconciliation_runs_latest`, mesmo que o run tenha falhado (nesse caso `category` e NULL e `txn_count` e 0).

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `reference_date` | date | Data de referencia |
| `run_id` | bigint | ID do run mais recente (qualquer status) |
| `run_status` | varchar | Status do run mais recente |
| `category` | varchar | Categoria de reconciliacao (NULL se run falhou) |
| `txn_count` | integer | Contagem de transacoes na categoria |
| `pct_of_total` | decimal | Percentual da categoria sobre o total do dia |
| `internal_amount_sum` | decimal | Soma dos valores internos |
| `processor_amount_sum` | decimal | Soma dos valores do processador |
| `abs_difference_sum` | decimal | Soma das diferencas absolutas |

---

### `gold_ops_reconciliation_trend` (view)

Grao: `reference_date x category`. Extende `gold_ops_reconciliation_daily` com media movel de 7 dias.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| *(todas de `gold_ops_reconciliation_daily`)* | | |
| `pct_of_total_7d_avg` | decimal | Media movel dos 7 dias anteriores de `pct_of_total` por categoria |

```sql
AVG(pct_of_total) OVER (
    PARTITION BY category
    ORDER BY reference_date
    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
)
```

---

### `gold_ops_run_history` (view)

Grao: `run_id x category`. Todos os runs (inclusive falhos e superseded), nao apenas o winning run. Um run sem resultados aparece com `category` NULL e `txn_count` 0.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `run_id` | bigint | ID do run |
| `reference_date` | date | Data de referencia |
| `file_name` | varchar | Arquivo processado |
| `run_status` | varchar | Status do run |
| `total_transactions` | integer | Total de transacoes declarado |
| `started_at` | timestamptz | Inicio do processamento |
| `completed_at` | timestamptz | Fim do processamento |
| `category` | varchar | Categoria de reconciliacao |
| `txn_count` | integer | Contagem na categoria |
| `pct_of_total` | decimal | Percentual sobre o total do run |

> Diferenca critica vs `gold_ops_reconciliation_daily`: este usa `silver_reconciliation_results` (sem filtro de winning-run), enquanto daily usa `silver_reconciliation_results_current` (apenas winning run).

---

### `gold_cfo_weekly_summary` (tabela)

Grao: `week_start x category`. Agregacao semanal ISO (Seg-Dom). Materializado como TABLE.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `week_start` | date | Segunda-feira da semana ISO |
| `week_end` | date | Domingo da semana (`week_start + 6 dias`) |
| `category` | varchar | Categoria de reconciliacao |
| `txn_count` | integer | Total de transacoes na semana/categoria |
| `amount_brl` | decimal | Volume em BRL: `COALESCE(processor_amount, internal_amount)` |

> `amount_brl` usa o valor confirmado pelo PaySettler quando disponivel; cai para o valor interno apenas em `UNRECONCILED_INTERNAL` (onde `processor_amount` e NULL por definicao).

---

### `gold_cfo_weekly_merchant_ranking` (tabela)

Grao: `week_start x merchant_id x category`. Apenas categorias nao-matched. Materializado como TABLE.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `week_start` | date | Segunda-feira da semana ISO |
| `merchant_id` | varchar | Identificador do merchant |
| `category` | varchar | `MISMATCHED`, `UNRECONCILED_PROCESSOR` ou `UNRECONCILED_INTERNAL` |
| `txn_count` | integer | Total de transacoes |
| `amount_brl` | decimal | Volume em BRL (`COALESCE(processor_amount, internal_amount)`) |
| `legal_name` | varchar | Razao social (pode ser NULL se merchant sem cadastro) |
| `trade_name` | varchar | Nome fantasia |

> Filtro `WHERE category IN ('MISMATCHED', 'UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')` — MATCHED nao aparece pois nao representa risco.

---

### `gold_compliance_ledger` (view)

Grao: `result_id` (transacao x run). Sem agregacao, sem filtro de winning-run. Registro completo para auditoria.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `result_id` | bigint | PK do resultado (`silver_reconciliation_results.id`) |
| `run_id` | bigint | ID do run |
| `reference_date` | date | Data de referencia |
| `file_name` | varchar | Arquivo processado |
| `started_at` | timestamptz | Inicio do run |
| `completed_at` | timestamptz | Fim do run |
| `run_status` | varchar | Status do run |
| `transaction_id` | varchar | UUID da transacao |
| `merchant_id` | varchar | Identificador do merchant |
| `legal_name` | varchar | Razao social |
| `document` | varchar | CNPJ |
| `category` | varchar | Categoria de reconciliacao |
| `internal_amount` | decimal(18,2) | Valor no sistema interno |
| `processor_amount` | decimal(18,2) | Valor no PaySettler |
| `difference` | decimal(18,2) | Diferenca absoluta |
| `created_at` | timestamptz | Data de criacao do resultado |

> Inclui runs superseded e falhos — permite reconstruir o estado da reconciliacao em qualquer ponto no tempo.

---

## Limitacoes Conhecidas

### Rebuild total do gold

`CREATE OR REPLACE TABLE` nas tabelas CFO re-escaneia toda a silver a cada execucao. Com o volume atual (~10k transacoes) isso leva menos de 1 segundo. Com a projecao de ~1.8B linhas em 18 meses, o rebuild levaria minutos a horas. Nao ha mecanismo de "fechar semana" que impeca o rebuild de sobrescrever snapshots historicos — a propriedade de snapshot imutavel do CFO e uma convencao, nao uma garantia do codigo.

### Sem triggering baseado em eventos

O pipeline e inteiramente CLI-pull: depende de `make` (ou cron externo) para ser acionado. Nao reage a "arquivo CSV chegou no S3" ou "Kafka topic tem mensagem nova". Uma falha no agendamento externo simplesmente para a cadeia sem alarme proprio.

### DuckDB single-process

Apenas um processo pode escrever no `warehouse.duckdb` por vez. Rodar `build-gold` enquanto `seed-silver` esta em progresso resulta em erro de lock. Em producao com multiplos pipelines paralelos (diferentes `reference_date`s), isso e um gargalo imediato.

### Sem particionamento de storage

Bronze e silver vivem num unico arquivo DuckDB. Nao ha particao por `reference_date` no filesystem. Toda leitura faz full scan — sem partition pruning.

### Outputs estaticos

Alerta Ops gera JSON no formato Slack Block Kit mas nao chama a API do Slack. Relatorio CFO gera HTML mas nao envia email. Em producao: webhook Slack + SMTP/SendGrid.

---

Para como escalar essa arquitetura, veja [arquitetura_scale.md](arquitetura_scale.md).
