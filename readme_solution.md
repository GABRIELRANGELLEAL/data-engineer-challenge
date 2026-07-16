# Solucao do Case - Pipeline de Reconciliacao de Liquidacoes

## Visao Geral

Pipeline de dados com arquitetura medalhao (Bronze / Silver / Gold) implementado em **Python 3.12 + DuckDB**, rodando 100% local via Docker Compose. O objetivo e transformar dados transacionais brutos (Parquet + CSV do PaySettler) em **produtos de dados** consumiveis por Operacoes, CFO e Compliance.

## Estrutura do Projeto

```
src/
  db.py                        # Conexao DuckDB centralizada
  a_bronze/                    # Ingestao e normalizacao de dados brutos
    cdc_transaction.py         #   CDC dedup de transacoes internas (Parquet)
    settlement_loader.py       #   Loader de CSVs do PaySettler (normaliza BRL, valida, dedup)
    reconciliation_runs.py     #   Landing de reconciliation_runs
    reconciliation_results.py  #   Landing de reconciliation_results
    enterprise_company.py      #   Landing de dados cadastrais
  b_silver/                    # Camada curada com CDC e views enriquecidas
    cdc_reconc.py              #   Dedup CDC para runs + results
    cdc_company.py             #   Dedup CDC para empresa
    build.py + sql/            #   Views: winning-run policy, enriquecimento com merchant
  c_gold/                      # Artefatos analiticos por consumidor
    build.py + sql/            #   6 artefatos (3 VIEWs Ops, 2 TABLEs CFO, 1 VIEW Compliance)
  observability/
    health.py                  #   Profiling generico (nulls, duplicatas, range de datas)
  products/                    # Geracao de outputs finais
    ops_alert.py               #   Alerta Slack (JSON Block Kit + SVG)
    cfo_report.py              #   Relatorio CFO (HTML com graficos SVG)
    ops_run_report.py          #   Historico de runs para Ops (HTML)
```

## Produtos de Dados

### Operacoes (Ops)
| Artefato | Tipo | O que responde |
|----------|------|----------------|
| `gold_ops_reconciliation_daily` | VIEW | Saude diaria: quantas transacoes por categoria, % de match, volumes |
| `gold_ops_reconciliation_trend` | VIEW | Media movel 7 dias por categoria -- detecta spikes |
| `gold_ops_run_history` | VIEW | Todos os runs (inclusive falhos e re-runs) por data |
| Alerta Ops (`_alert.json` + `_chart.svg`) | Output | JSON pronto para Slack + grafico de barras SVG |

### CFO
| Artefato | Tipo | O que responde |
|----------|------|----------------|
| `gold_cfo_weekly_summary` | TABLE | Volume semanal agregado por categoria em BRL |
| `gold_cfo_weekly_merchant_ranking` | TABLE | Ranking de merchants com mais discrepancias por semana |
| Relatorio CFO (`_cfo_report.html`) | Output | HTML com KPIs, graficos de volume diario e ranking de risco |

### Compliance
| Artefato | Tipo | O que responde |
|----------|------|----------------|
| `gold_compliance_ledger` | VIEW | Registro completo de toda transacao em todo run -- sem filtro de winning-run, sem agregacao |

## Como Rodar

**Pre-requisito:** Docker e Docker Compose instalados.

```bash
# 1. Subir o container
docker compose up -d --build

# 2. Pipeline completo (gera dados + bronze + silver + gold + outputs)
make run_pipeline
```

Isso executa toda a cadeia: geracao de dados sinteticos (10k linhas), carga bronze, dedup silver, construcao gold e geracao de todos os outputs em `outputs/`.

### Passo a passo manual (se preferir)

```bash
# Bronze - ingestao
make load-cdc-transactions
make load-settlement
make load-reconciliation-runs
make load-reconciliation-results
make load-enterprise-company

# Silver - CDC + views curadas
make build-silver

# Gold - artefatos analiticos
make build-gold

# Outputs
make run-alerts
make run-cfo-report
make run-ops-run-report
```

### Inspecionar o banco

```bash
docker compose exec pipeline python -c "
import duckdb; conn = duckdb.connect('data/warehouse.duckdb')
print(conn.execute('SHOW TABLES').fetchdf())
"
```

### Testes

```bash
make test   # 21 testes (pytest) - gerador sintetico + settlement loader
```

## Decisoes-Chave

- **Winning-run policy**: o run mais recente com status COMPLETED por `reference_date` e o "vencedor". Um run FAILED nao esconde um COMPLETED anterior.
- **Gold TABLE vs VIEW**: CFO usa TABLEs (snapshot congelado), Ops e Compliance usam VIEWs (sempre refletem o estado atual).
- **Idempotencia**: todo step usa `CREATE OR REPLACE` ou `INSERT OR REPLACE`. Reprocessar nao duplica dados.
- **Append-only na silver**: re-runs criam novos registros, nunca sobrescrevem. Compliance ve tudo; gold ve so o winner.

## Documentacao Complementar

- [Arquitetura detalhada](arquitetura.md)
- [Arquitetura em escala](arquitetura_scale.md)
- [Glossario de dominio](domain-glossary.md)
- [Guia Docker](docker_guide.md)
