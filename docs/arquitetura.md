# Arquitetura — Settlement Reconciliation Data Platform

---

## Parte 3.1 — Desenho de Arquitetura

### Visão local (implementação atual)

```
docs/sample-data/                       scripts/generate_sample_data.py
  ├── transactions_batch_*.parquet  ─┐
  ├── reconciliation_runs.parquet    │    (gerador sintético para testes
  ├── reconciliation_results.parquet │     de escala — mesmo schema)
  ├── enterprise_company.parquet    ─┤
  └── paysettler/settlement_*.csv  ──┘
              │
              ▼
    ┌────────────────────────────────────────────────────────────┐
    │                  Bronze Layer (DuckDB)                     │
    │  raw_transactions            ← cdc_transaction.py          │
    │    (única exceção: já sai deduplicada — CDC aplicado aqui) │
    │  raw_paysettler_settlements  ← settlement_loader.py        │
    │  raw_reconciliation_runs     ← reconciliation_runs.py      │
    │  raw_reconciliation_results  ← reconciliation_results.py   │
    │  raw_enterprise_company      ← enterprise_company.py       │
    │    (landing puro — sem CDC, sem regra de negócio)          │
    └──────────────────┬───────────────────────────────────────┘
                       │
                       ▼
    ┌────────────────────────────────────────────────────────────┐
    │                  Silver Layer (DuckDB)                     │
    │  silver_reconciliation_runs    (CDC dedup, CREATE OR       │
    │  silver_reconciliation_results  REPLACE — idempotente)     │
    │  silver_enterprise_company     (CDC dedup, upsert)         │
    │                    ← cdc_reconc.py / cdc_company.py        │
    │                                                            │
    │  silver_reconciliation_runs_latest      (VIEW curada)      │
    │  silver_reconciliation_results_current  (VIEW curada,      │
    │    winning-run + enriquecida com dados do merchant)        │
    │                    ← build.py                              │
    └──────────────────┬───────────────────────────────────────┘
                       │
                       ▼
    ┌────────────────────────────────────────────────────────────┐
    │                  Gold Layer (DuckDB)                       │
    │  gold_ops_reconciliation_daily   (VIEW, 1 linha/dia mesmo  │
    │                                    se o run mais recente    │
    │                                    falhou — run_status)     │
    │  gold_ops_reconciliation_trend   (VIEW)                    │
    │  gold_cfo_weekly_summary         (TABLE snapshot)          │
    │  gold_cfo_weekly_merchant_ranking (TABLE snapshot)         │
    │  gold_compliance_ledger          (VIEW, sem filtro de      │
    │                                    winning-run — histórico  │
    │                                    completo p/ auditoria)   │
    └──────────────────┬───────────────────────────────────────┘
                       │
                       ▼
                  Produtos (leem só gold)
    ops_alert.py → {date}_alert.json + {date}_chart.svg   (Slack card, Ops)
    cfo_report.py → {start}_{end}_cfo_report.html          (CFO)
    gold_compliance_ledger — consultado via SQL direto     (Compliance)
```

**Todos os artefatos persistem em `data/warehouse.duckdb` (arquivo local).**

---

### Visão de produção (como levaríamos isso a sério)

```
PostgreSQL (settlement_db)
        │
        │  Debezium CDC
        ▼
    Apache Kafka
   ┌────┴─────┐
   │  Topics  │
   │  txns    │  ← consumido pelo bronze loader
   │  company │
   └────┬─────┘
        │  Kafka Consumer (Python / Flink)
        ▼
  ┌────────────────────────────────────────┐
  │   Object Storage (S3 / GCS)            │
  │   bronze/                              │
  │   ├── transactions/year=Y/month=M/...  │  ← Parquet particionado
  │   └── settlements/reference_date=D/... │     por data de referência
  └────────────────┬───────────────────────┘
                   │  dbt / Spark (ou DuckDB em nó maior)
                   ▼
  ┌────────────────────────────────────────┐
  │   Data Warehouse (BigQuery / Redshift) │
  │   silver_*  →  gold_*                  │
  └────────────────┬───────────────────────┘
                   │
          ┌────────┴────────────┐
          ▼                     ▼
     BI Tool (Metabase /    Airflow / Dagster
     Looker / Superset)     (agendamento + alertas)
          │                     │
          ▼                     ▼
    Dashboards Ops         Slack (Ops alert)
    Relatório CFO          Email (CFO report)
    Ledger Compliance
```

**Por que DuckDB localmente e não em produção:**  
DuckDB é single-process. Para ~5M txns/mês com um único arquivo local, ele é extremamente eficiente. Acima de alguns milhões de linhas por dia, o gargalo deixa de ser o SQL e passa a ser o I/O serial de um único nó — ponto onde Spark ou um warehouse columnar distribuído se justificam.

---

## Parte 3.2 — Troubleshooting: Dashboards sem dados desde sexta-feira

Segunda de manhã. Dashboards de reconciliação sem dados. Protocolo de investigação:

### 1. Verificar o status dos runs na silver layer (< 1 min)

```sql
-- Conectar diretamente ao DuckDB local ou ao warehouse
SELECT reference_date, status, started_at, completed_at, total_transactions
FROM silver_reconciliation_runs
WHERE reference_date >= CURRENT_DATE - 4  -- desde sexta
ORDER BY reference_date DESC, id DESC;
```

**O que procurar:** se `status = 'FAILED'` ou se nenhuma linha existe para sexta/sábado/domingo → o pipeline parou ou nunca rodou.

### 2. Checar os logs do container

```bash
docker compose logs pipeline --since 72h | grep -E "ERROR|FAILED|Quality gate|reference_date"
```

**O que procurar:**
- `status = 'FAILED'` nos runs listados no passo 1 → algo interrompeu o processamento antes da conclusão (upstream)
- Erro do loader bronze (`FileNotFoundError`, `ValueError` de schema) → o CSV/parquet do PaySettler não chegou ou não bateu com o schema esperado
- Stack trace Python → bug de código ou schema inesperado no arquivo

### 3. Verificar se o CSV do PaySettler chegou no bronze

```sql
SELECT reference_date, COUNT(*) AS rows, _source_file
FROM raw_paysettler_settlements
WHERE reference_date >= CURRENT_DATE - 4
GROUP BY reference_date, _source_file
ORDER BY reference_date DESC;
```

**O que procurar:** se a tabela está vazia para a data esperada → o arquivo não foi processado (ou nunca chegou). Checar o diretório de entrada do arquivo CSV.

### 4. Verificar se a gold layer foi reconstruída

```sql
-- gold_cfo_weekly_summary é TABLE — tem dados das semanas anteriores?
SELECT week_start, SUM(txn_count) FROM gold_cfo_weekly_summary
GROUP BY week_start ORDER BY week_start DESC LIMIT 5;
```

**O que procurar:** se os dados param antes de sexta → `build-gold` (que já roda `build-silver` como pré-requisito) não rodou após o pipeline de silver, ou o silver estava vazio quando rodou.

### 5. Decisão de escalação

| Diagnóstico | Ação | Escalação |
|-------------|------|-----------|
| CSV não chegou | Confirmar com PaySettler se arquivo foi enviado | Acionar time de integração com PaySettler |
| Run com `status = 'FAILED'` no upstream | Investigar a data específica junto ao time responsável pela extração/CDC | Avisar Ops sobre janela sem dados |
| Bug de código (stack trace) | Corrigir e rerrodar `seed-silver`/`seed-company`; como o seed é sempre `CREATE OR REPLACE` a partir da bronze, basta rodar de novo — não há estado incremental pra reconciliar | PR de hotfix + runbook para Ops |
| gold layer não reconstruída | Rerrodar `make build-gold`; sem perda de dados (bronze/silver intactos) | Apenas comunicar SLA de delay |

**Regra geral:** até o passo 4, o diagnóstico é solo. Escalação só acontece quando o problema está fora do perímetro do pipeline (CSV do PaySettler, falha de infra, bug não óbvio).

---

## Parte 3.3 — Escalabilidade: 5M transações/dia em 18 meses

### Onde o desenho atual quebra primeiro

**1. DuckDB single-process não escala horizontalmente**

DuckDB não tem modo distribuído. A 5M txns/dia (≈1,8B/ano em `reconciliation_results`), um único nó começa a sentir o peso nas queries de gold que fazem full-scan da silver. O `CREATE OR REPLACE TABLE` das tabelas de CFO re-escaneia **toda** a história a cada build. Isso passa de segundos para minutos.

*Solução:* substituir DuckDB por um warehouse columnar particionado (BigQuery, Redshift, ou DuckDB sobre Parquet no S3 com particionamento por `reference_date`). As queries SQL são portáveis — o schema e os SQLs do gold layer não mudam, só o executor.

**2. Filesystem local como armazenamento é single point of failure**

`data/warehouse.duckdb` é um arquivo local. Sem replicação, sem backups automáticos, sem acesso concorrente de múltiplos processos. Em produção, qualquer falha de disco destrói bronze e silver — hoje ambas só existem dentro desse único arquivo.

*Solução:* separar armazenamento de compute. Bronze e silver persistem em Parquet no S3 particionado por `reference_date`. O warehouse (BigQuery/Redshift) lê de lá. Isso dá durabilidade, acesso concorrente, e permite re-seedar qualquer data sem impactar o nó de compute.

**3. Rebuild incremental do gold não existe**

As tabelas `gold_cfo_weekly_summary` e `gold_cfo_weekly_merchant_ranking` são recriadas inteiras a cada `build-gold`. A 1,8B linhas na silver, isso é inviável em produção. Além disso, reprocessar uma data passada sobrescreve snapshots semanais já "fechados" — quebrando a propriedade de snapshot imutável que o CFO depende.

*Solução:* introduzir um mecanismo de "fechamento de semana" — uma flag ou partição que marca semanas passadas como imutáveis. O rebuild então só toca a semana corrente. Com Airflow/Dagster, isso seria um sensor: "se a semana fechou (`week_end < today`), não rebuildar essa partição".

### O que não quebra (e por quê)

- **Schema da silver:** chave `run_id` + `reference_date`, particionável por data — esse design escala naturalmente, é só particionar por data no storage. `CREATE OR REPLACE TABLE` no seed é hoje um full-rebuild da silver a partir da bronze; em escala isso viraria um `MERGE`/upsert incremental por partição, mas o schema em si não muda.
- **Winning-run policy:** centralizada em `silver_reconciliation_results_current` (uma única `ROW_NUMBER()` sobre runs por data, O(número de runs) — não de resultados). Antes era uma CTE copiada em cada SQL da gold; agora todo artefato de gold lê dessa view, então o custo não se multiplica por artefato.
- **SQL do gold:** portável para qualquer SQL engine. A migração de DuckDB para BigQuery é uma mudança de driver, não de lógica.
