# Arquitetura - Pipeline de Reconciliacao de Liquidacoes

## Visao Geral

O pipeline segue a **arquitetura medalhao** (Bronze / Silver / Gold) rodando inteiramente em DuckDB local, orquestrado via Makefile e containerizado com Docker Compose.

```
  Fontes Externas                        Produtos de Dados
  ─────────────────                      ─────────────────
  Parquet (CDC)   ──┐                ┌── Alerta Ops (JSON + SVG)
  CSV PaySettler  ──┼── Bronze ── Silver ── Gold ──┼── Relatorio CFO (HTML)
  Parquet (seed)  ──┘                └── Relatorio Run History (HTML)
```

## Camadas

### Bronze - Ingestao e Normalizacao

Responsabilidade: carregar dados brutos no DuckDB com transformacoes minimas.

| Modulo | Fonte | Tabela | Logica |
|--------|-------|--------|--------|
| `cdc_transaction.py` | `transactions_batch_*.parquet` | `raw_transactions` | CDC dedup (`ROW_NUMBER` por `_timestamp DESC`, exclui `Op='D'`). Trata schema drift entre batches via `union_by_name`. |
| `settlement_loader.py` | CSVs diarios do PaySettler | `raw_paysettler_settlements` | Normaliza amounts BRL (`R$ 32.245,91` -> `32245.91`). Valida campos obrigatorios. Dedup intra-arquivo. `INSERT OR REPLACE` com PK `(transaction_id, reference_date)`. |
| `reconciliation_runs.py` | `reconciliation_runs.parquet` | `raw_reconciliation_runs` | Landing puro -- `CREATE OR REPLACE TABLE`. |
| `reconciliation_results.py` | `reconciliation_results.parquet` | `raw_reconciliation_results` | Landing puro -- `CREATE OR REPLACE TABLE`. |
| `enterprise_company.py` | `enterprise_company.parquet` | `raw_enterprise_company` | Landing puro -- `CREATE OR REPLACE TABLE`. |

**Quality gates no bronze:**
- Settlement loader rejeita arquivos com colunas faltantes, vazios, ou com amounts zero/negativos
- Rows com `_timestamp` NULL sao descartadas no CDC
- Arquivos invalidos sao pulados sem abortar o batch

### Silver - CDC + Views Curadas

Responsabilidade: deduplicar CDC, aplicar regras de negocio e enriquecer dados.

**Tabelas (CDC dedup):**
- `silver_reconciliation_runs` -- dedup de `raw_reconciliation_runs` (ultimo estado por `id`)
- `silver_reconciliation_results` -- dedup de `raw_reconciliation_results`
- `silver_enterprise_company` -- dedup de `raw_enterprise_company`

**Views curadas:**
- `silver_reconciliation_runs_latest` -- ultimo run por `reference_date` (independente de status). Usado pelo gold para surfacear falhas.
- `silver_reconciliation_results_current` -- resultados do **winning run** (ultimo COMPLETED por `reference_date`). Enriquecido com dados do merchant via LEFT JOIN com `silver_enterprise_company`.

**Winning-run policy:** o run mais recente com `status = 'COMPLETED'` para cada `reference_date` e o vencedor. Um run FAILED nao oculta um COMPLETED anterior. Isso permite reruns seguros -- basta executar novamente sem risco de perder o estado anterior.

### Gold - Artefatos Analiticos

Responsabilidade: materializar visoes otimizadas por consumidor.

| Artefato | Tipo | Grao | Consumidor | SQL |
|----------|------|------|------------|-----|
| `gold_ops_reconciliation_daily` | VIEW | `reference_date x category` | Ops | Breakdown diario com contagem, percentual, volumes |
| `gold_ops_reconciliation_trend` | VIEW | `reference_date x category` | Ops | Adiciona media movel 7 dias para deteccao de anomalias |
| `gold_ops_run_history` | VIEW | `run_id x category` | Ops | Todo run (inclusive falho/superseded), nao so o winner |
| `gold_cfo_weekly_summary` | TABLE | `week_start x category` | CFO | Agregacao semanal ISO (Seg-Dom). `amount_brl = COALESCE(processor, internal)` |
| `gold_cfo_weekly_merchant_ranking` | TABLE | `week_start x merchant_id x category` | CFO | Ranking de merchants com mais discrepancias |
| `gold_compliance_ledger` | VIEW | `result_id` | Compliance | Registro completo sem filtro de winning-run |

**TABLE vs VIEW:**
- CFO: TABLEs para congelar snapshots (o CFO nao quer que o relatorio da semana passada mude retroativamente)
- Ops: VIEWs para sempre refletir o estado mais recente
- Compliance: VIEW para capturar todo o historico incluindo reruns

### Outputs - Produtos Finais

| Produto | Modulo | Saida | Descricao |
|---------|--------|-------|-----------|
| Alerta Ops | `ops_alert.py` | `{date}_alert.json` + `_chart.svg` | JSON Slack Block Kit com status, match rate e alertas ativos. Triggers: run nao-COMPLETED ou categoria acima do threshold E 1.5x da media 7 dias. |
| Relatorio CFO | `cfo_report.py` | `{start}_{end}_cfo_report.html` | HTML com KPIs (volume total BRL, total txns), graficos SVG de volume diario e ranking top-N merchants de risco. |
| Run History Ops | `ops_run_report.py` | `{date}_ops_run_report.html` | HTML com historico de todos os runs (8 dias), incluindo re-runs e falhas. |

## Fluxo de Dados

```
1. make load-*                  Fontes -> Bronze (raw_*)
2. make seed-silver             Bronze -> Silver (silver_*)  [CDC dedup]
3. make build-silver            Silver tables -> Silver views [winning-run + enriquecimento]
4. make build-gold              Silver views -> Gold          [artefatos analiticos]
5. make run-alerts/cfo/ops      Gold -> Outputs              [JSON, SVG, HTML]
```

Cada step e idempotente: `CREATE OR REPLACE TABLE/VIEW` ou `INSERT OR REPLACE`. O pipeline inteiro pode ser re-executado com `make run_pipeline`.

## Observabilidade

`src/observability/health.py` roda apos cada carga e reporta:
- Row count
- Null rate por coluna
- Duplicatas na PK informada
- Range de datas

O Makefile chama `print_health.py` automaticamente apos cada step de carga, dando visibilidade imediata sobre a qualidade dos dados em cada camada.

## Idempotencia

| Cenario | Comportamento |
|---------|--------------|
| Reprocessar mesmo CSV | `INSERT OR REPLACE` na PK -- sem duplicatas |
| Re-run de reconciliacao | Cria novo run (append-only). Gold usa o winning-run. |
| Rebuild completo do gold | `CREATE OR REPLACE TABLE/VIEW` -- resultado identico |
| Falha no meio do pipeline | Steps anteriores ja persistidos. Basta re-executar do ponto de falha. |

## Limitacoes Conhecidas

1. **Rebuild total do gold a cada execucao.** `CREATE OR REPLACE TABLE` nas tabelas CFO re-escaneia toda a silver. Com ~1.8B linhas em 18 meses, isso leva minutos/horas.

2. **Sem triggering baseado em eventos.** O pipeline e CLI-pull: depende de cron externo ou execucao manual. Nao reage a "arquivo chegou no S3".

3. **DuckDB single-process.** Apenas um processo pode escrever no `warehouse.duckdb` por vez. Execucoes concorrentes resultam em erro de lock.

4. **Sem particionamento de storage.** Bronze e silver vivem num unico arquivo DuckDB. Sem particao por `reference_date` no filesystem.

5. **Outputs estaticos.** Alerta Ops gera JSON mas nao chama a API do Slack. Relatorio CFO gera HTML mas nao envia email.
