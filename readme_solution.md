# Solucao do Case - Pipeline de Reconciliacao de Liquidacoes

## Visao Geral

Pipeline de dados com arquitetura medalhao (Bronze / Silver / Gold) implementado em **Python 3.12 + DuckDB**, rodando 100% local via Docker Compose. O objetivo e transformar dados transacionais brutos (Parquet + CSV do PaySettler) em **produtos de dados** consumiveis por Operacoes, CFO e Compliance.

## Estrutura do Projeto

```
src/
  db.py                        # Conexao DuckDB centralizada
  a_bronze/                    # Ingestao e normalizacao de dados brutos
    cdc_transaction.py         #   CDC dedup de transacoes internas (Parquet)
    settlement_loader.py       #   Loader de CSVs do PaySettler
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

**Alerta diario para Slack** (`outputs/{date}_alert.json` + `{date}_chart.svg`)

O produto principal de Ops e um alerta automatico que avalia a saude da reconciliacao para uma data de referencia e gera dois arquivos:

- **JSON no formato Slack Block Kit** — pronto para ser enviado via webhook. Contem: status geral (OK ou CRITICAL), status do run, total de transacoes, match rate, breakdown por categoria com emoji e contagem, e lista de alertas ativos quando houver.
- **Grafico SVG de barras** — visualizacao das taxas de cada categoria (MATCHED, MISMATCHED, UNRECONCILED_PROCESSOR, UNRECONCILED_INTERNAL) com cores semanticas.

O alerta dispara em dois cenarios: (1) o run mais recente para a data nao tem status COMPLETED, ou (2) uma categoria ultrapassa seu threshold configuravel **E** esta acima de 1.5x a media movel de 7 dias (evitando falsos positivos em dias com volume naturalmente maior). Os thresholds sao configuraveis via variaveis de ambiente (`ALERT_MISMATCHED_THRESHOLD=5%`, `ALERT_UNRECONCILED_THRESHOLD=10%`, `ALERT_TREND_SPIKE_MULT=1.5`).

**Relatorio de historico de runs** (`outputs/{date}_ops_run_report.html`)

Relatorio HTML que mostra **todos** os runs de reconciliacao dos ultimos 8 dias — inclusive reruns e tentativas que falharam. Diferente do alerta (que foca na foto do dia), esse relatorio serve para investigacao e audit: permite ao time de Ops ver se houve reruns, se um FAILED foi seguido de um COMPLETED, e qual foi o breakdown de cada tentativa. Organizado por data com tabelas mostrando run_id, arquivo processado, status (pill colorido), timestamps de inicio/fim, total de transacoes e breakdown por categoria.

**Tabelas analiticas de suporte:**

| Artefato | Tipo | Grao | O que responde |
|----------|------|------|----------------|
| `gold_ops_reconciliation_daily` | VIEW | `reference_date x category` | Saude diaria: quantas transacoes por categoria, % de match, volumes internos e do processador |
| `gold_ops_reconciliation_trend` | VIEW | `reference_date x category` | Media movel 7 dias por categoria — alimenta a logica de deteccao de spikes do alerta |
| `gold_ops_run_history` | VIEW | `run_id x category` | Todos os runs (inclusive falhos e re-runs) por data — alimenta o relatorio de historico |

---

### CFO

**Relatorio financeiro consolidado** (`outputs/{start}_{end}_cfo_report.html`)

Relatorio HTML completo cobrindo todo o periodo disponivel, pensado para ser enviado por email ao CFO. Contem:

- **KPIs de headline** — volume total em BRL e total de transacoes, exibidos em cards destacados no topo.
- **Volume por categoria (total)** — grafico SVG de barras horizontais mostrando a distribuicao de volume BRL entre MATCHED, MISMATCHED e as duas categorias UNRECONCILED, acompanhado de tabela com valores exatos.
- **Volume por categoria (diario)** — dois graficos SVG de linhas separados: um para volume MATCHED e outro para as categorias nao-matched (MISMATCHED, UNRECONCILED_PROCESSOR, UNRECONCILED_INTERNAL). Cada ponto do grafico tem tooltip com data e valor. Abaixo, tabela dia-a-dia com colunas por categoria, total e contagem de transacoes.
- **Ranking top-N de merchants por risco** — tabela com os merchants que acumularam mais volume em categorias nao-matched (MISMATCHED + UNRECONCILED), somado ao longo de todo o periodo. O N e configuravel via `CFO_REPORT_TOP_N` (default: 10). Mostra nome fantasia (ou razao social), contagem de transacoes e volume BRL.

**Tabelas analiticas de suporte:**

| Artefato | Tipo | Grao | O que responde |
|----------|------|------|----------------|
| `gold_cfo_weekly_summary` | TABLE | `week_start x category` | Volume semanal agregado por categoria em BRL (ISO Seg-Dom). Materializado como TABLE para congelar snapshots. |
| `gold_cfo_weekly_merchant_ranking` | TABLE | `week_start x merchant_id x category` | Ranking de merchants com mais discrepancias por semana — alimenta o ranking do relatorio |

---

### Compliance

**Ledger de auditoria** (`gold_compliance_ledger`)

VIEW que expoe o registro completo de **toda transacao em todo run de reconciliacao**, sem nenhum filtro de winning-run e sem agregacao. Cada linha representa um resultado de reconciliacao individual, preservando: id do resultado, id do run, data de referencia, transaction_id, merchant_id, categoria, valores interno e do processador, diferenca, e todos os timestamps.

Diferente dos artefatos de Ops e CFO que usam o winning-run (ultimo COMPLETED) para mostrar a "verdade atual", o compliance ledger preserva deliberadamente **todo o historico**, incluindo runs que foram superseded por reruns e runs que falharam. Isso garante rastreabilidade completa para auditorias: e possivel reconstruir o estado da reconciliacao em qualquer ponto no tempo e verificar como os resultados mudaram entre runs.

A VIEW nao tem output HTML/JSON associado — o consumo se da diretamente via queries SQL contra o warehouse, ou via export para Parquet/CSV conforme demanda da equipe de compliance.

---

## Como Rodar

**Pre-requisito:** Docker e Docker Compose instalados.

```bash
# 1. Subir o container
docker compose up -d --build

# 2. Pipeline completo (gera dados + bronze + silver + gold + outputs)
make run_pipeline 
```

Isso executa toda a cadeia: geração de dados sinteticos (10k linhas), carga bronze, dedup silver, construcao gold e geracao de todos os outputs em `outputs/`.

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

### Winning-run policy

Cada `reference_date` pode ter multiplos runs de reconciliacao (por exemplo, um run que falhou seguido de um reprocessamento). O pipeline define o **winning run** como o run mais recente com `status = 'COMPLETED'` para aquela data. Isso significa que:

- Um run FAILED nao esconde um COMPLETED anterior — se o reprocessamento falhou, o resultado anterior continua valido.
- Reruns sao seguros: basta executar novamente sem risco de perder dados. O novo COMPLETED se torna o winner automaticamente.
- A implementacao esta na view `silver_reconciliation_results_current`, que todos os artefatos gold de Ops e CFO consomem.
- Compliance deliberadamente **ignora** essa policy — o ledger mostra todos os runs, inclusive os superseded, para rastreabilidade completa.

### Gold TABLE vs VIEW

A escolha entre TABLE e VIEW para cada artefato gold nao e arbitraria — reflete a necessidade de cada consumidor:

- **CFO (TABLEs):** O CFO precisa que o relatorio da semana passada mostre os mesmos numeros se reaberto hoje. Uma VIEW recalcularia com base no estado atual da silver (que pode ter recebido reruns), mudando retroativamente os numeros. A TABLE congela o snapshot no momento do `build-gold`.
- **Ops (VIEWs):** Ops quer sempre o estado mais recente. Se um rerun corrigiu resultados, a view reflete imediatamente.
- **Compliance (VIEW):** O ledger mostra todo o historico sem filtro. Como nao ha agregacao nem winning-run, uma VIEW e suficiente — o resultado e deterministico dado o conteudo da silver.

### Idempotencia em todas as camadas

O pipeline inteiro pode ser re-executado quantas vezes for necessario sem duplicar ou corromper dados:

- **Bronze:** `CREATE OR REPLACE TABLE` para Parquet loads. `INSERT OR REPLACE` na PK `(transaction_id, reference_date)` para CSVs do PaySettler — recarregar o mesmo arquivo sobrescreve os registros anteriores.
- **Silver:** `CREATE OR REPLACE TABLE` para as tabelas CDC e `CREATE OR REPLACE VIEW` para as views curadas. Qualquer rebuild produz o mesmo resultado.
- **Gold:** `CREATE OR REPLACE TABLE/VIEW`. O rebuild e total (nao incremental), garantindo consistencia.
- **Falha parcial:** se o pipeline falha no meio (ex: silver completa mas gold nao), basta re-executar do ponto de falha. Steps anteriores ja estao persistidos e o `CREATE OR REPLACE` do step atual recalcula sem efeito colateral.

### Append-only na silver

Reruns de reconciliacao criam **novos registros** na silver — nunca sobrescrevem os anteriores. Isso tem duas consequencias importantes:

- **Compliance ve tudo:** o ledger (`gold_compliance_ledger`) acessa todos os runs, inclusive os superseded. E possivel reconstruir o estado da reconciliacao em qualquer ponto no tempo.
- **Gold ve so o winner:** os artefatos de Ops e CFO consomem `silver_reconciliation_results_current`, que filtra pelo winning run. Reruns anteriores nao poluem as metricas operacionais.

Essa separacao resolve um conflito real: Ops e CFO precisam de uma unica verdade (o resultado mais recente e correto), enquanto Compliance precisa de rastreabilidade completa. Ambos consomem a mesma silver, filtrada de forma diferente.

### Deteccao de anomalias no alerta Ops

O alerta nao dispara apenas quando uma categoria ultrapassa um threshold fixo — ele exige **duas condicoes simultaneas**: o threshold E um spike acima de 1.5x a media movel de 7 dias. Isso evita falsos positivos em dias com volume naturalmente diferente (ex: sexta-feira com mais transacoes pode ter uma taxa de UNRECONCILED levemente maior sem que isso indique um problema). Se nao ha historico de 7 dias (primeiros dias do pipeline), o threshold sozinho e suficiente para disparar.

### CDC e schema drift no bronze

Os dados de transacoes internas chegam como Parquet com campos CDC do Debezium (`Op` = I/U/D, `_timestamp`). O pipeline aplica `ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC)` e exclui `Op='D'` para obter o snapshot atual. Alem disso, os dois batches de transacoes tem schemas diferentes (`batch_2` tem `payment_method`, `batch_1` nao). O DuckDB lida com isso via `union_by_name=true`, preenchendo NULL nas colunas ausentes.

## Observabilidade

O pipeline inclui um modulo generico de health check (`src/observability/health.py`) que roda automaticamente apos cada step de carga no Makefile. Ele introspecciona o schema da tabela (sem precisar de configuracao por tabela) e reporta:

- **Row count** — quantas linhas a tabela tem apos a carga
- **Null rate por coluna** — quais colunas tem valores nulos e em que percentual
- **Duplicatas na PK** — se as colunas informadas como PK tem registros duplicados (OK ou WARNING)
- **Range de datas** — min/max da primeira coluna de data/timestamp encontrada

Cada target do Makefile chama `print_health.py` logo apos a carga, dando visibilidade imediata. Exemplo de saida apos carregar os CSVs do PaySettler:

```
=== Data Health: raw_paysettler_settlements ===
Rows:      21,847
Columns:   9
PK check (transaction_id, reference_date): OK
Date range (settled_at): 2025-03-01 00:01:12 -> 2025-04-14 23:58:33
Nulls:     none
========================================
```

E um exemplo com problemas detectados (nulls e duplicatas):

```
=== Data Health: raw_transactions ===
Rows:      10,230
Columns:   11
PK check (transaction_id): WARNING — 3 duplicate key(s)
Date range (_timestamp): 2025-02-24 00:00:01 -> 2025-04-14 23:59:58
Nulls:
  payment_method              4,891 (47.8%)
  description                    12 (0.1%)
========================================
```

O health check nao bloqueia o pipeline (e informativo, nao um gate), mas torna problemas de qualidade visiveis no momento em que acontecem — em vez de serem descobertos dias depois quando um dashboard mostra dados estranhos. Em producao, esses sinais alimentariam alertas automaticos (ver [arquitetura em escala](docs/arquitetura_scale.md)).

Para rodar manualmente contra qualquer tabela:

```bash
docker compose exec pipeline python scripts/print_health.py <tabela> --pk <col1,col2>
```

## Documentacao Complementar

- [Arquitetura detalhada](docs/arquitetura.md)
- [Arquitetura em escala](docs/arquitetura_scale.md)
- [Glossario de dominio](docs/domain-glossary.md)
- [Guia Docker](docs/docker_guide.md)
