# Arquitetura em Escala

Como levar o pipeline de reconciliacao do ambiente local (DuckDB + Docker Compose) para producao. O volume assumido hoje e de ~5M transacoes/**mes**, com projecao de chegar a **~5M transacoes/dia em 18 meses** (~1.8B linhas/ano em `reconciliation_results`). A arquitetura proposta usa **Databricks**, **Kafka** e **Airflow**.

## Onde o Desenho Atual Quebra

| Gargalo | Volume atual | Projecao 5M/dia | Impacto |
|---------|-------------|-----------------|---------|
| DuckDB single-file | ~10k txns | ~150M txns/mes | Arquivo DuckDB cresce para dezenas de GB. Write-lock impede concorrencia. |
| Rebuild total do gold | ~1s | Minutos a horas | CFO tables re-escaneiam toda a silver a cada build. SLA de atualizacao impossivel. |
| Sem particionamento | Tudo num arquivo | Idem | Nenhuma poda de particao. Full-scan obrigatorio em todo read. |
| Orquestracao via Make | OK para dev | Fragil | Sem retry, sem dependencia explicita entre runs, sem alerta de SLA. |
| Container unico | OK para dev | Insuficiente | Sem isolamento entre bronze/silver/gold. Falha em um step para tudo. |
| Ingestao batch-only | CSVs manuais | Latencia de horas | Ops so ve o resultado da reconciliacao no dia seguinte. Sem near-real-time. |

**O que quebra primeiro:** o write-lock single-process do DuckDB. Antes mesmo do volume virar problema de performance, a necessidade de processar multiplos `reference_date`s em paralelo (backfills, reruns) esbarra no lock de escrita — e um limite de concorrencia, nao de tamanho. Em seguida vem o rebuild total do gold, que cresce linearmente com o historico acumulado na silver.

---

## Arquitetura Proposta

```
  PaySettler (SFTP/API) ──> Kafka topic: raw_settlements ──┐
  Sistema Interno (CDC) ──> Kafka topic: raw_transactions ──┼──> Databricks
  Cadastro Merchants    ──> Kafka topic: raw_companies ─────┘    (Lakehouse)
                                                                     │
                                 Airflow                             │
                              (orquestrador)                         │
                                    │                                │
                    ┌───────────────┼───────────────┐                │
                    ▼               ▼               ▼                │
              ┌──────────┐   ┌──────────┐   ┌──────────┐            │
              │  Bronze  │   │  Silver  │   │   Gold   │            │
              │  Delta   │──>│  Delta   │──>│  Delta   │            │
              │  Tables  │   │  Tables  │   │  Tables  │            │
              └──────────┘   └──────────┘   └──────────┘            │
                    │               │               │                │
                    ▼               ▼               ▼                │
              S3 / ADLS        S3 / ADLS      Databricks SQL         │
              Delta Lake       Delta Lake      Warehouse             │
                                                    │                │
                                     ┌──────────────┼────────┐      │
                                     ▼              ▼        ▼      │
                                  Looker /      Slack /   Compliance│
                                  Metabase      Email     Export    │
```

---

## Kafka: Ingestao em Streaming

### Por que Kafka

No desenho atual, os dados chegam como arquivos (CSVs do PaySettler, Parquets de CDC). Em producao com 5M txns/dia, essa abordagem tem dois problemas: (1) latencia — Ops so ve o resultado da reconciliacao horas depois, e (2) fragilidade — se o arquivo nao chega, ninguem sabe ate o dia seguinte.

Kafka resolve ambos:

### Topics

| Topic | Producer | Formato | Particao | Descricao |
|-------|----------|---------|----------|-----------|
| `raw_settlements` | PaySettler (via connector ou API gateway) | Avro/JSON | `merchant_id` | Cada liquidacao publicada individualmente, em vez de acumulada num CSV diario |
| `raw_transactions` | Debezium CDC no banco transacional | Avro | `transaction_id` | Eventos de insert/update/delete capturados em tempo real |
| `raw_companies` | Debezium CDC na tabela `enterprise_company` | Avro | `merchant_id` | Alteracoes cadastrais |
| `reconciliation_events` | Pipeline (apos gold) | Avro | `reference_date` | Eventos de conclusao de reconciliacao — consumidos por alertas |

### Modelo de Ingestao

Duas opcoes, dependendo do SLA de Ops:

**Near-real-time (recomendado para Ops):**
- Spark Structured Streaming (via Delta Live Tables) le direto dos topics `raw_settlements` e `raw_transactions` com o Kafka source
- Bronze atualizado a cada micro-batch (intervalo configuravel: 1-15 min)
- Silver e Gold atualizados via DLT com streaming

**Batch diario (suficiente para CFO e Compliance):**
- Kafka Connect S3 sink escreve Parquet particionado no S3 a cada hora
- Databricks Auto Loader ingere incrementalmente os arquivos que chegam no S3 (Auto Loader le arquivos em object storage, nao Kafka — por isso ele entra neste caminho, nao no streaming)
- Airflow DAG diaria consolida e processa o batch completo
- Mesmo fluxo bronze >> silver >> gold, mas com dados ja no S3

Na pratica, ambos coexistem: Ops consome near-real-time, CFO e Compliance consomem o batch consolidado.

---

## Databricks: Processamento e Storage

### Por que Databricks

O DuckDB atual e single-process e single-file. Databricks resolve:
- **Delta Lake** no S3/ADLS com ACID transactions, partition pruning e time travel
- **Spark distribuido** para processar 5M+ txns/dia com paralelismo
- **Unity Catalog** para governanca, linhagem e controle de acesso por persona (Ops vs CFO vs Compliance)
- **Databricks SQL** como warehouse para servir dashboards com baixa latencia

### Delta Tables por Camada

#### Bronze

| Tabela | Fonte | Particao | Merge Key | Descricao |
|--------|-------|----------|-----------|-----------|
| `bronze.raw_transactions` | Kafka `raw_transactions` / Parquet | `reference_date` | `transaction_id, _timestamp` | CDC events — todas as versoes mantidas |
| `bronze.raw_settlements` | Kafka `raw_settlements` / CSV | `reference_date` | `transaction_id, reference_date` | Liquidacoes do PaySettler. MERGE INTO para idempotencia |
| `bronze.raw_reconciliation_runs` | Kafka / Parquet | `reference_date` | `id` | Runs de reconciliacao |
| `bronze.raw_reconciliation_results` | Kafka / Parquet | `reference_date` | `id` | Resultados de reconciliacao |
| `bronze.raw_enterprise_company` | Kafka `raw_companies` | — | `id` | Dados cadastrais (SCD Type 2 via Delta) |

Delta Lake garante que `MERGE INTO` (equivalente ao `INSERT OR REPLACE` do DuckDB) e atomico e idempotente, mesmo com writers concorrentes.

#### Silver

| Tabela | Origem | Logica | Descricao |
|--------|--------|--------|-----------|
| `silver.reconciliation_runs` | `bronze.raw_reconciliation_runs` | CDC dedup (ultimo `_timestamp` por `id`, exclui `Op='D'`) | Mesmo SQL do DuckDB, agora via Spark |
| `silver.reconciliation_results` | `bronze.raw_reconciliation_results` | CDC dedup identica | |
| `silver.enterprise_company` | `bronze.raw_enterprise_company` | CDC dedup + SCD Type 2 | Historico de alteracoes cadastrais |
| `silver.reconciliation_results_current` | `silver.reconciliation_results` + `silver.reconciliation_runs` + `silver.enterprise_company` | Winning-run policy + enriquecimento merchant | Mesma logica da view atual, materializada como Delta table para performance |

**Diferenca chave vs implementacao atual:** em vez de views que recalculam a cada leitura, a silver materializa `reconciliation_results_current` como Delta table com **MERGE incremental** — so processa `reference_date`s novas ou re-processadas.

#### Gold

| Tabela | Tipo | Atualizacao | Descricao |
|--------|------|-------------|-----------|
| `gold.ops_reconciliation_daily` | Delta table | Incremental (MERGE por `reference_date, category`) | Mesma logica, mas nao recalcula dias anteriores |
| `gold.ops_reconciliation_trend` | Delta table | Incremental (recalcula ultimos 8 dias para a media movel) | |
| `gold.ops_run_history` | Delta table | Incremental (MERGE por `run_id, category`) | |
| `gold.cfo_weekly_summary` | Delta table | Incremental (MERGE por `week_start, category`). Semanas fechadas sao imutaveis. | |
| `gold.cfo_weekly_merchant_ranking` | Delta table | Incremental (MERGE por `week_start, merchant_id, category`) | |
| `gold.compliance_ledger` | Delta table | Append-only (novas linhas por run) | Nunca sobrescreve — historico completo |

**Time travel do Delta** substitui a necessidade de TABLE vs VIEW: o CFO pode consultar `SELECT * FROM gold.cfo_weekly_summary VERSION AS OF <timestamp>` para ver o snapshot exato de qualquer momento.

---

## Airflow ou Data Factory: Orquestracao

### Por que Airflow (e nao so Databricks Jobs)

Databricks Jobs orquestra notebooks e Spark jobs, mas o pipeline tem dependencias fora do Databricks: Kafka health checks, Slack webhooks, SendGrid emails, exports S3 para compliance. Airflow e o orquestrador "acima" que coordena tudo.

### DAG Principal

```
DAG: reconciliation_daily
Schedule: 06:00 UTC (ou trigger via Kafka consumer lag sensor)

  kafka_lag_check ─────> bronze_load_settlements ──> bronze_load_transactions
       │                        │                           │
       │                        └──────────┬────────────────┘
       ▼                                   ▼
  kafka_health_alert              silver_cdc_dedup
  (se lag > threshold)                    │
                                          ▼
                                 silver_build_current
                                          │
                                          ▼
                                    gold_incremental
                                          │
                              ┌───────────┼───────────────┐
                              ▼           ▼               ▼
                         ops_alert   cfo_report    compliance_export
                         (Slack)     (SendGrid)    (S3 Parquet)
```

### Capacidades

| Capacidade | Implementacao |
|------------|---------------|
| **Retry automatico** | 3 tentativas com backoff exponencial por task |
| **SLA alerts** | Se gold nao completou ate 07:00 UTC, PagerDuty notifica Ops |
| **Backfill** | `airflow dags backfill --start-date 2025-01-01 --end-date 2025-03-31` reprocessa historico |
| **Sensores** | `KafkaConsumerLagSensor` detecta dados novos; `S3KeySensor` como fallback para CSVs batch |
| **Parametrizacao** | DAG recebe `reference_date` como parametro — cada run e independente |
| **Concorrencia** | `max_active_runs=3` permite processar multiplos dias em paralelo (Delta Lake suporta writers concorrentes, diferente do DuckDB) |

### Integracao Databricks

Cada task de processamento (bronze, silver, gold) usa o `DatabricksSubmitRunOperator` do Airflow:

```python
silver_build = DatabricksSubmitRunOperator(
    task_id="silver_cdc_dedup",
    databricks_conn_id="databricks_default",
    existing_cluster_id="{{ var.value.cluster_id }}",
    notebook_task={
        "notebook_path": "/pipelines/silver/cdc_dedup",
        "base_parameters": {"reference_date": "{{ ds }}"},
    },
    retries=3,
    retry_delay=timedelta(minutes=5),
)
```

---

## Qualidade de Dados

### Databricks Expectations (Delta Live Tables)

Expectations sao expressoes booleanas avaliadas **por linha**:

```python
@dlt.expect_or_fail("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect("valid_category", "category IN ('MATCHED','MISMATCHED','UNRECONCILED_PROCESSOR','UNRECONCILED_INTERNAL')")
```

Checks que dependem de agregacao (duplicatas na PK, match rate, volume diario) nao cabem numa expectation por linha — rodam como task separada na DAG (query agregada pos-carga que falha ou alerta conforme a severidade).

| Check | Tipo | Acao |
|-------|------|------|
| `transaction_id` NOT NULL | Hard fail | Pipeline para. Airflow notifica. |
| Duplicatas na PK | Hard fail | Pipeline para. |
| Amount > 0 | Drop row | Linha descartada, metrica incrementada |
| Match rate > 80% | Soft alert | Pipeline continua, Slack notificado |
| Volume diario ± 2 std | Soft alert | Pipeline continua, metrica logada |

Resultados persistidos em `audit.data_quality_runs` para historico.

---

## Observabilidade em Producao

| Sinal | Ferramenta | Descricao |
|-------|------------|-----------|
| Metricas de pipeline | Datadog + Databricks metrics | Duracao por step, rows processadas, erro rate, Spark stage metrics |
| Kafka lag | Datadog Kafka integration | Lag por topic/consumer group — indica se a ingestao esta atrasada |
| Alertas de SLA | Airflow + PagerDuty | Gold nao completou ate 07:00 |
| Qualidade de dados | DLT Expectations | Checks automaticos pos-carga com historico em Delta |
| Logs estruturados | Databricks + CloudWatch | JSON logs com `reference_date`, `step`, `row_count`, `spark_job_id` |
| Linhagem | Unity Catalog | Linhagem automatica entre tabelas Delta — quem produziu, quando, a partir de que |

---

## Outputs em Producao

| Produto | Atual (local) | Producao |
|---------|---------------|----------|
| Alerta Ops | JSON no disco | **Slack webhook** via Airflow `SlackWebhookOperator`. Em near-real-time: Kafka consumer em `reconciliation_events` triggera alerta direto. |
| Relatorio CFO | HTML no disco | **Email via SendGrid/SES** com HTML inline, disparado por Airflow no final da DAG semanal |
| Compliance | VIEW no DuckDB | **Export Delta para Parquet no S3** + acesso via Databricks SQL ou Athena. Retencao configuravel por politica do Unity Catalog. |
| Dashboards | Nenhum | **Databricks SQL dashboards** ou **Looker/Metabase** conectado ao Databricks SQL warehouse |


## Troubleshooting: "Dashboards sem dados desde sexta"

Cenario da Parte 3.2 do case — segunda-feira de manha, dashboards vazios.

### Investigacao

1. **Airflow UI** — verificar se a DAG de sexta/sabado/domingo rodou. Se nao: o `KafkaConsumerLagSensor` nao detectou dados novos? A DAG estava pausada? Houve deploy no fim de semana que pausou tudo?

2. **Kafka lag** — checar no Datadog/Confluent Control Center se os topics `raw_settlements` e `raw_transactions` receberam mensagens. Se o lag e zero e nao houve mensagens: o problema e no producer (PaySettler nao enviou, Debezium caiu).

3. **Logs do bronze no Databricks** — o job rodou? Se sim, falhou em que? Checar no Spark UI: OOM, schema change inesperado, permissao S3. Filtrar logs por `reference_date=2025-XX-XX`.

4. **DLT Expectations** — algum quality check bloqueou o pipeline? Ver tabela `audit.data_quality_runs` e o dashboard de DLT no Databricks.

5. **Silver/Gold** — se bronze OK, verificar se silver/gold completaram:
   ```sql
   SELECT MAX(reference_date) FROM silver.reconciliation_results_current
   ```

6. **Dashboard** — se gold OK, o problema e na camada de consumo: Databricks SQL warehouse desligou? Credenciais do Looker/Metabase expiraram? Cache stale?

### Como descobrir onde deu ruim

Investigar da fonte para o consumidor: **Kafka → Bronze → Silver → Gold → Dashboard**, parando no primeiro step com problema.

