# Arquitetura em Escala

Como levar o pipeline de reconciliacao do ambiente local (DuckDB + Docker Compose) para producao com **~5M transacoes/dia** (~1.8B/ano).

## Onde o Desenho Atual Quebra

| Gargalo | Volume atual | Projecao 5M/dia | Impacto |
|---------|-------------|-----------------|---------|
| DuckDB single-file | ~10k txns | ~150M txns/mes | Arquivo DuckDB cresce para dezenas de GB. Write-lock impede concorrencia. |
| Rebuild total do gold | ~1s | Minutos a horas | CFO tables re-escaneiam toda a silver a cada build. SLA de atualizacao impossivel. |
| Sem particionamento | Tudo num arquivo | Idem | Nenhuma poda de particao. Full-scan obrigatorio em todo read. |
| Orquestracao via Make | OK para dev | Fragil | Sem retry, sem dependencia explicita entre runs, sem alerta de SLA. |
| Container unico | OK para dev | Insuficiente | Sem isolamento entre bronze/silver/gold. Falha em um step para tudo. |

## Arquitetura Proposta para Escala

```
                                         ┌──────────────────┐
  PaySettler CSV  ──> S3 Landing Zone ──>│  Airflow / Dagster │
  CDC Parquet     ──> S3 Landing Zone ──>│  (orquestrador)    │
                                         └────────┬─────────┘
                                                  │
                              ┌────────────────────┼────────────────────┐
                              ▼                    ▼                    ▼
                        ┌──────────┐        ┌──────────┐        ┌──────────┐
                        │  Bronze  │        │  Silver  │        │   Gold   │
                        │  Spark / │───────>│  Spark / │───────>│  Spark / │
                        │  DuckDB  │        │  DuckDB  │        │  DuckDB  │
                        └──────────┘        └──────────┘        └──────────┘
                              │                    │                    │
                              ▼                    ▼                    ▼
                        S3 Parquet            S3 Parquet          Warehouse
                        particionado         particionado        (BigQuery /
                        por date             por date            Redshift /
                                                                 Snowflake)
                                                                     │
                                                      ┌──────────────┤
                                                      ▼              ▼
                                                   Looker /      Slack /
                                                   Metabase      Email
```

## Mudancas por Componente

### 1. Storage: S3 + Parquet Particionado

**Problema:** DuckDB single-file nao escala alem de ~100M linhas sem degradar.

**Solucao:**
- Bronze e Silver em **Parquet no S3**, particionado por `reference_date`
- Formato: `s3://datalake/silver/reconciliation_results/reference_date=2025-03-15/*.parquet`
- Beneficio: leituras filtram por particao (partition pruning), escrita e paralela por data
- Compressao Snappy/Zstd reduz storage em ~5x vs CSV

**Gold** pode ser materializado em um **warehouse analitico** (BigQuery, Redshift, Snowflake) para servir dashboards com baixa latencia. Alternativamente, DuckDB continua viavel para gold se os dados ja estiverem pre-agregados na silver.

### 2. Processamento: Spark ou DuckDB Distribuido

**Problema:** DuckDB e single-writer e roda em um unico processo.

**Opcoes:**

| Opcao | Quando usar |
|-------|-------------|
| **DuckDB por particao** | Se o volume por `reference_date` cabe em memoria (~170k txns/dia). Cada task do Airflow abre seu proprio DuckDB in-memory, le/escreve Parquet no S3. Sem lock. |
| **Spark (EMR / Dataproc)** | Se o volume por particao excede memoria ou se precisa de shuffle (joins grandes). Cluster elastico. |

A recomendacao para 5M/dia: **DuckDB por particao** e suficiente. Cada dia tem ~170k txns, que cabe em <1 GB de memoria. Spark so se justifica com joins cross-partition ou agregacoes que cubram meses.

### 3. Orquestracao: Airflow ou Dagster

**Problema:** Makefile nao tem retry, dependencia explicita, alertas de SLA, nem backfill.

**Solucao:**
- **Airflow** (ou Dagster) com DAG diaria parametrizada por `reference_date`
- Dependencia: `bronze >> silver >> gold >> outputs`
- **Retry automatico** com backoff exponencial (3 tentativas por task)
- **SLA alerts**: se o gold nao completou ate 07:00, notifica Ops
- **Backfill**: `airflow dags backfill --start-date 2025-01-01 --end-date 2025-03-31` reprocessa o historico sem tocar nos dados atuais
- **Sensores**: S3 sensor detecta chegada do CSV do PaySettler e triggera a DAG automaticamente

```
DAG: reconciliation_daily (schedule: 06:00 UTC)

  s3_sensor_csv  >>  bronze_load_csv  >>  bronze_load_cdc
                           │                     │
                           └────────┬────────────┘
                                    ▼
                              silver_build
                                    │
                                    ▼
                               gold_build
                                    │
                          ┌─────────┼─────────┐
                          ▼         ▼         ▼
                     ops_alert  cfo_report  compliance_export
```

### 4. Gold Incremental

**Problema:** `CREATE OR REPLACE TABLE` reconstroi o gold inteiro a cada execucao.

**Solucao:**
- **VIEWs (Ops, Compliance):** continuam como views -- sem custo de rebuild, leitura direta da silver.
- **TABLEs (CFO):** incremental com `INSERT ... WHERE week_start = <semana_atual>` + `DELETE` da semana corrente antes de re-inserir (upsert por semana).
- Semanas anteriores sao **imutaveis** -- o build so toca a semana aberta.
- Para rebuild historico completo: flag `--full-refresh` que faz o `CREATE OR REPLACE` original.

### 5. Qualidade de Dados: Great Expectations / dbt Tests

**Problema:** Health checks atuais sao informativos mas nao bloqueiam o pipeline.

**Solucao:**
- **Great Expectations** (ou dbt tests) com checks que **bloqueiam** o pipeline:
  - Null rate em `transaction_id` = 0% (hard fail)
  - Duplicatas na PK = 0 (hard fail)
  - Match rate > 80% (soft alert, nao bloqueia)
  - Volume diario dentro de 2 desvios-padrao do historico (soft alert)
- Resultados dos checks persistidos em tabela `data_quality_runs` para auditoria

### 6. Observabilidade em Producao

| Sinal | Ferramenta | Descricao |
|-------|------------|-----------|
| Metricas de pipeline | Datadog / CloudWatch | Duracao por step, rows processadas, erro rate |
| Alertas de SLA | Airflow + PagerDuty | Gold nao completou ate 07:00 |
| Qualidade de dados | Great Expectations | Checks automaticos pos-carga |
| Logs estruturados | CloudWatch Logs | JSON logs com `reference_date`, `step`, `row_count` |
| Linhagem | OpenLineage / Datahub | Quem produziu qual tabela, quando, a partir de que |

### 7. Outputs em Producao

| Produto | Atual | Producao |
|---------|-------|----------|
| Alerta Ops | JSON no disco | **Slack webhook** via Airflow operator |
| Relatorio CFO | HTML no disco | **Email via SendGrid/SES** com HTML inline |
| Compliance | VIEW no DuckDB | **Export Parquet para S3** + acesso via Athena/BigQuery |
| Dashboards | Nenhum | **Looker/Metabase** conectado ao warehouse |

## Estimativa de Custos (AWS, 5M txns/dia)

| Componente | Servico | Estimativa mensal |
|------------|---------|-------------------|
| Storage S3 | S3 Standard | ~$5-15 (Parquet comprimido) |
| Processamento | Lambda ou ECS Fargate | ~$50-100 (DuckDB por particao) |
| Orquestracao | MWAA (Managed Airflow) | ~$300-400 |
| Warehouse | BigQuery on-demand | ~$50-200 (depende de queries) |
| Monitoramento | Datadog | ~$100-200 |
| **Total** | | **~$500-900/mes** |

Com Spark (EMR), o custo de processamento sobe para ~$300-500/mes mas oferece margem para 10x o volume.

## Plano de Migracao

| Fase | Duracao | O que muda |
|------|---------|------------|
| **1. Storage** | 2 semanas | Mover bronze/silver para S3 Parquet particionado. DuckDB le do S3 via httpfs. |
| **2. Orquestracao** | 2 semanas | Airflow DAG substituindo Makefile. Mesma logica Python, diferente trigger. |
| **3. Gold incremental** | 1 semana | Upsert semanal nas tabelas CFO. Views nao mudam. |
| **4. Outputs reais** | 1 semana | Slack webhook + SendGrid. Templates ja existem (JSON/HTML). |
| **5. Observabilidade** | 1 semana | Metricas, alertas de SLA, data quality gates. |
| **6. Warehouse** | 2 semanas | BigQuery/Redshift para gold. Dashboards Looker/Metabase. |

**Total: ~9 semanas** para ir de Docker Compose local ate producao com 5M txns/dia.

## Troubleshooting: "Dashboards sem dados desde sexta"

Cenario da Parte 3.2 do case -- segunda-feira de manha, dashboards vazios.

**Investigacao (com a arquitetura escalada):**

1. **Airflow UI** -- verificar se a DAG de sexta/sabado/domingo rodou. Se nao: sensor S3 nao detectou o CSV? Ou a DAG estava pausada?
2. **Logs do bronze** -- o CSV chegou no S3? Se sim, o loader falhou? Checar logs estruturados com `reference_date=2025-XX-XX`.
3. **Data quality checks** -- algum check bloqueou o pipeline? Ver tabela `data_quality_runs`.
4. **Silver/Gold** -- se bronze OK, verificar se silver build completou. `SELECT MAX(reference_date) FROM silver_reconciliation_results` mostra ate onde os dados chegaram.
5. **Conexao do dashboard** -- se gold OK, o problema e no Looker/Metabase (credencial expirada, cache stale).

**Regra:** investigar da fonte para o consumidor (S3 -> bronze -> silver -> gold -> dashboard), parando no primeiro step com problema.

**Quando escalar:** se em 30 minutos nao identificou a causa raiz, acionar o time de plataforma. Se o problema e no processador externo (CSV nao enviado), acionar o contato do PaySettler.
