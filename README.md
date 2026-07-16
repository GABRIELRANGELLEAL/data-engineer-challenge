# Case Técnico — Engenheiro(a) de Dados Sênior / Especialista

## Contexto de Negócio

Você está entrando no time de dados de uma fintech que processa liquidações de pagamentos para merchants. A empresa opera o **Settlement Reconciliation Service** — um serviço em produção que:

1. Recebe diariamente um **arquivo CSV** do processador de pagamentos externo (**PaySettler**) com as transações liquidadas nas últimas 24h
2. Compara essas transações com os **registros internos** do sistema
3. Categoriza cada transação e persiste os resultados em um **PostgreSQL** (`settlement_db`)

O serviço funciona bem, mas **todos os dados vivem apenas no banco transacional**. O time de operações precisa saber se a taxa de discrepância está subindo. O CFO quer o volume transacionado por dia. Compliance precisa de histórico de auditoria. **Ninguém tem acesso estruturado a nada disso hoje.**

### Sua Missão

Construir uma solução que transforme esses dados em **produtos de dados** consumíveis pelo negócio. Cabe a você decidir como.

---

## Domínio: Reconciliação de Liquidações

> O glossário completo está em [`docs/domain-glossary.md`](docs/domain-glossary.md). Leia antes de começar.

### Tabelas Fonte (`settlement_db`)

**`transactions`** — Transações internas do sistema

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | bigint | PK |
| `transaction_id` | uuid | Identificador único da transação |
| `merchant_id` | varchar | Identificador do merchant |
| `amount` | decimal | Valor da transação (BRL) |
| `currency` | varchar | Moeda (ISO 4217) |
| `status` | varchar | `COMPLETED`, `PENDING`, `FAILED` |
| `description` | varchar | Descrição da transação |
| `created_at` | timestamp | Data de criação |
| `updated_at` | timestamp | Última atualização |

**`reconciliation_runs`** — Execuções de reconciliação

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | bigint | PK |
| `reference_date` | date | Data de referência do arquivo (dia de negócio) |
| `file_name` | varchar | Nome do arquivo processado |
| `status` | varchar | `IN_PROGRESS`, `COMPLETED`, `FAILED` |
| `total_transactions` | integer | Total de transações no arquivo |
| `started_at` | timestamp | Início do processamento |
| `completed_at` | timestamp | Fim do processamento |
| `created_at` | timestamp | Data de criação do registro |

**`reconciliation_results`** — Resultados da reconciliação

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | bigint | PK |
| `run_id` | bigint | FK → `reconciliation_runs.id` |
| `transaction_id` | uuid | Identificador da transação |
| `merchant_id` | varchar | Identificador do merchant |
| `category` | varchar | `MATCHED`, `MISMATCHED`, `UNRECONCILED_PROCESSOR`, `UNRECONCILED_INTERNAL` |
| `internal_amount` | decimal | Valor no sistema interno (null se unreconciled_processor) |
| `processor_amount` | decimal | Valor no PaySettler (null se unreconciled_internal) |
| `difference` | decimal | Diferença absoluta entre valores |
| `created_at` | timestamp | Data de criação |

**`enterprise_company`** — Dados cadastrais dos merchants

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | bigint | PK |
| `merchant_id` | varchar | Código do merchant |
| `legal_name` | varchar | Razão social |
| `trade_name` | varchar | Nome fantasia |
| `document` | varchar | CNPJ |
| `primary_cnae` | varchar | CNAE principal |
| `created_at` | timestamp | Data de criação |
| `updated_at` | timestamp | Última atualização |

### Arquivo CSV do PaySettler

O processador externo envia diariamente um CSV com as transações liquidadas.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `transaction_id` | uuid | Identificador da transação |
| `merchant_id` | varchar | Identificador do merchant |
| `amount` | decimal | Valor liquidado |
| `currency` | varchar | Moeda (ISO 4217) |
| `settled_at` | datetime (ISO 8601, UTC) | Data/hora da liquidação |
| `processor_reference` | varchar | Referência interna do processador |
| `status` | varchar | `SETTLED` ou `REVERSED` |

---

## Stack

- **Linguagem:** Python 3.12+
- **Processamento:** DuckDB
- **Conteinerização:** Docker + Docker Compose
- **Armazenamento:** Filesystem local

A solução deve **rodar localmente** com `docker-compose up`.

---

## Dados de Exemplo

Na pasta `docs/sample-data/` você encontrará:

- `transactions_batch_1.parquet` — Transações internas (lote 1)
- `transactions_batch_2.parquet` — Transações internas (lote 2)
- `reconciliation_runs.parquet` — Execuções de reconciliação
- `reconciliation_results.parquet` — Resultados categorizados
- `settlement_paysettler.csv` — Arquivo CSV do PaySettler
- `enterprise_company.parquet` — Dados cadastrais de merchants

---

## Como Começar

1. **Faça um fork** deste repositório para sua conta pessoal do GitHub
2. Clone o seu fork e trabalhe nele normalmente
3. Suba o ambiente:

```bash
docker compose up -d --build
# ou: make up
```

Um `Makefile` no repositório expõe atalhos para as operações mais comuns (`make help` lista os alvos disponíveis). O uso é opcional.

---

## Requisitos

### Parte 1 — Modelagem e Analytics

Usando os dados disponíveis, **modele e implemente tabelas analíticas** que atendam os seguintes consumidores:

- **Operações:** precisa acompanhar a saúde das reconciliações no dia a dia
- **CFO:** precisa de visão consolidada do volume financeiro
- **Compliance:** precisa de rastreabilidade e histórico para auditorias

**A forma e o conteúdo das tabelas são decisão sua.** Traduza as dores acima em métricas e tabelas analíticas (fatos e dimensões) que você defenderia em produção. Documente, para cada consumidor:

- **Quais perguntas** seu modelo consegue responder (e quais deliberadamente não)
- **Por que** essas e não outras (priorização, tradeoffs de escopo vs tempo)

Entregue também exemplos de queries rodando contra suas tabelas que respondem as perguntas que você propôs. Queremos ver seu raciocínio de produto, não só SQL.

---

### Parte 2 — Pipeline de Dados

Implemente o pipeline que processa as fontes de dados disponíveis e prepara o insumo para as tabelas analíticas da Parte 1.

O volume em `docs/sample-data/` é pequeno de propósito — assuma **~5M transações/mês em produção** e projete para crescer. Se quiser validar na prática como seu pipeline se comporta em escala, use o gerador descrito na seção _"Dados de Exemplo e Geração de Volume"_.

Critérios de qualidade (como você os atende é decisão sua — documente suas escolhas):

- **Correção:** a saída do pipeline bate com o contrato de dados que você propôs
- **Idempotência:** executar o pipeline para a mesma data de referência duas vezes não pode duplicar registros nem corromper as tabelas. Considere como isso se manifesta na sua CLI, nas tabelas finais e em falhas no meio da execução
- **Observabilidade:** em produção, quando algo quebrar, que sinais você precisaria para diagnosticar rapidamente?
- **Qualidade de dados:** quais checks você adicionaria para proteger os consumidores de downstream de dados incorretos? O que faria o pipeline parar versus apenas alertar?
- **Testabilidade:** como você valida automaticamente que uma mudança no código não quebra o comportamento?

Stack obrigatória: **DuckDB** para processamento, **Python 3.12+**, **Docker Compose** para subir o ambiente.

---

### Parte 3 — Arquitetura

#### 3.1 Desenho de Arquitetura

**Explique e/ou desenhe** como você levaria esses dados do banco transacional até o consumo pelo negócio em produção.

#### 3.2 Troubleshooting

Segunda-feira de manhã. Ops abre um chamado: os dashboards de reconciliação estão sem dados desde sexta-feira à noite. Você é o primeiro a chegar.

Como você investigaria? Que artefatos do próprio pipeline você consultaria? Até onde iria antes de acionar mais alguém?

#### 3.3 Escalabilidade

Hoje o volume é pequeno, mas o negócio projeta chegar em **~5M transações/dia** em 18 meses (≈1,8B/ano em `reconciliation_results`). Como você prepararia a plataforma para esse crescimento? Onde o desenho atual quebra primeiro?

---

## Dados de Exemplo e Geração de Volume

O repositório oferece duas fontes de dado. **A escolha de volume e de quais fontes usar é sua.**

### 1. Fixture pequeno — `docs/sample-data/`

Cerca de 1.000 transações internas e ~500 liquidações. Pensado para exploração, desenvolvimento local e como insumo default dos exemplos de queries da Parte 1.

### 2. Gerador sintético — `scripts/generate_sample_data.py`

Produz o mesmo schema do fixture (CDC `Op`/`_timestamp`, schema drift entre batches, padrões realistas de dados sujos) em volume arbitrário. Útil para validar seu pipeline em escala — por exemplo, ao redor dos ~5M transações/mês assumidos na Parte 2.

**Você define o volume e as características.** O script é parametrizável:

```bash
# dataset médio para smoke em escala (1M transações, 90 dias)
python scripts/generate_sample_data.py --rows 1000000 --days 90 --out /tmp/medium

# dataset alinhado à projeção de ~5M transações/mês
python scripts/generate_sample_data.py --rows 5000000 --days 30 --out /tmp/month
```

Flags principais:

| Flag | Descrição | Default |
|------|-----------|---------|
| `--rows N` | Total de transações internas a gerar | `1000000` |
| `--days N` | Janela temporal em dias | `90` |
| `--merchants N` | Número de merchants | `500` |
| `--seed N` | Semente determinística | `42` |
| `--out PATH` | Diretório de saída | `docs/sample-data` |

Rode `python scripts/generate_sample_data.py --help` para a lista completa. Dentro do container, `make generate` (10k linhas) e `make generate-large` (1M linhas) são atalhos prontos.

---

## Uso de IA

Fique à vontade para usar ferramentas de IA durante o desafio. Se usar, documente no item _"Ferramentas de IA utilizadas"_ da entrega quais ferramentas e para quê. Se mantiver prompts, configs (`CLAUDE.md`, `.cursorrules`, etc.) ou transcrições no repositório, melhor — ajuda a entender seu processo.

---

## Entrega

1. Ao finalizar, **abra um Pull Request do seu fork para o repositório original** (branch `main`)
2. Preencha as seções abaixo no README do seu PR:

### Como rodar

**Pré-requisito:** Docker e Docker Compose instalados.

```bash
# 1. Subir o container
docker compose up -d --build

# 2. Seed da silver layer — carrega os dados históricos de exemplo
make seed-silver      # reconciliation_runs + reconciliation_results
make seed-company     # enterprise_company (dados cadastrais dos merchants)

# 3. (Opcional) Rodar uma reconciliação adicional para uma data específica
#    Primeiro, carregar o CSV do PaySettler no bronze:
docker compose exec pipeline python -m src.bronze.settlement_loader \
    docs/sample-data/settlement_paysettler.csv 2025-03-15
#    Depois, rodar a reconciliação:
docker compose exec pipeline python -m src.silver.reconcile 2025-03-15

# 4. Construir a gold layer (views e tabelas analíticas)
make build-gold

# 5. Gerar artefatos de saída
make run-alerts        # outputs/{date}_alert.json + _chart.svg
make run-cfo-report    # outputs/{start}_{end}_cfo_report.html

# 6. (Opcional) Rodar os testes
make test

# 7. (Opcional) Gerar dados sintéticos em escala
make generate          # 10k linhas em docs/sample-data
make generate-large    # 1M linhas em docs/sample-data
```

O banco DuckDB persiste em `data/warehouse.duckdb`. Para inspecionar diretamente:

```bash
docker compose exec pipeline python -c "
import duckdb; conn = duckdb.connect('data/warehouse.duckdb')
print(conn.execute('SHOW TABLES').fetchdf())
"
```

---

### Premissas e decisões

**Ambiguidades encontradas e como foram resolvidas:**

1. **Um CSV por `reference_date` ou múltiplos?** O enunciado descreve um arquivo diário. Assumi um arquivo por data de referência. O schema de `raw_paysettler_settlements` usa `(transaction_id, reference_date)` como PK — recarregar o mesmo arquivo é idempotente (INSERT OR REPLACE), mas dois arquivos para a mesma data seriam mergeados (comportamento documentado, não bloqueado).

2. **Status `REVERSED` no CSV do PaySettler.** O enunciado menciona `SETTLED` e `REVERSED`. A reconciliação filtra apenas `SETTLED` (linhas `REVERSED` são carregadas no bronze mas excluídas do join). Justificativa: uma transação revertida não está "liquidada" para fins de reconciliação — compará-la com o registro interno geraria UNRECONCILED_PROCESSOR artificiais.

3. **Tolerância de R$ 0,01 implementada como `ABS(diff) <= 0.01`.** Exatamente conforme o glossário. Valores como `0.005` (possível com arredondamento de IOF) são classificados como MATCHED.

4. **Silver append-only.** Reprocessar uma data cria um novo run — nunca sobrescreve. Downstream usa o "winning-run" (latest COMPLETED per `reference_date`) para estado atual. Compliance usa todos os runs. Essa decisão torna reruns seguros sem locks ou transações distribuídas.

5. **Gold CFO como TABLE, não VIEW.** O CFO precisa de um snapshot semanal que não mude retroativamente. Uma VIEW sobre a silver sempre refletiria reruns passados — o CFO veria números diferentes ao reabrir o relatório da semana anterior. A TABLE congela o estado no momento do build.

6. **Transações com `status != 'COMPLETED'` excluídas da janela interna.** Apenas transações `COMPLETED` do sistema interno são incluídas na reconciliação. `PENDING` e `FAILED` não devem aparecer como UNRECONCILED_INTERNAL — elas simplesmente não participam do escopo.

---

### Visão geral da arquitetura

Arquitetura medalhão de 3 camadas rodando inteiramente em DuckDB local:

```
Fontes (Parquet + CSV)
        │
        ▼
   Bronze Layer          raw_transactions, raw_paysettler_settlements
        │                Deduplicação, normalização de amount, CDC
        ▼
   Silver Layer          silver_reconciliation_runs/results/enterprise_company
        │                Append-only, quality gates, audit trail
        ▼
   Gold Layer            5 artefatos analíticos por consumidor
        │                (2 VIEWs Ops, 2 TABLEs CFO, 1 VIEW Compliance)
        ▼
   Outputs               Slack alert JSON + SVG, HTML email CFO
```

Veja o desenho completo e as decisões de produção em [`docs/arquitetura.md`](docs/arquitetura.md).

Documentação dos produtos de dados por consumidor (quais perguntas cada artefato responde): [`docs/produtos-de-dados.md`](docs/produtos-de-dados.md).

Exemplos de queries rodando contra as tabelas gold: [`docs/exemplo-queries.md`](docs/exemplo-queries.md).

Schema técnico da gold layer e decisões de design: [`docs/gold-schema.md`](docs/gold-schema.md).

---

### Extensibilidade

**Cenário:** adicionar um segundo processador de liquidação (ex.: `PayBoss`) com schema parecido mas não idêntico ao PaySettler.

**Arquivos que mudam:**

| Arquivo | Mudança |
|---------|---------|
| `src/bronze/payboss_loader.py` | **NOVO** — ~100 linhas. Copia a estrutura de `settlement_loader.py`, adapta os nomes de colunas do PayBoss e cria `raw_payboss_settlements`. A normalização de amount já é uma função reutilizável. |
| `src/silver/reconcile.py` | **~10 linhas alteradas** — a CTE `processor` do SQL de reconciliação precisa de um parâmetro `source` que seleciona entre `raw_paysettler_settlements` e `raw_payboss_settlements`. Alternativamente, criar uma view `raw_settlements_unified` que faz UNION das duas fontes — nesse caso `reconcile.py` não muda nada. |
| `Makefile` | **2-3 linhas** — target `seed-payboss` para carregar os dados de exemplo. |
| `tests/` | **1 novo notebook** — `test_payboss_loader.ipynb` seguindo o padrão dos existentes. |

**Total:** 1 arquivo novo (~100 linhas), 1 arquivo com ~10 linhas alteradas (ou zero, se usar a abordagem de view unificada), 2-3 linhas no Makefile.

A gold layer, os SQLs analíticos e os outputs **não mudam** — eles consomem silver, que já é agnóstica à fonte.

---

### Limites do desenho

1. **Rebuild total do gold a cada execução.** `CREATE OR REPLACE TABLE` nas tabelas CFO re-escaneia toda a silver history. Com ~1,8B linhas em 18 meses, isso passa de segundos para minutos/horas. Não há mecanismo de "fechar semana" que impeça o rebuild de sobrescrever snapshots históricos — a propriedade de snapshot imutável do CFO é uma convenção, não uma garantia do código.

2. **Nenhum mecanismo de triggering baseado em eventos.** O pipeline é inteiramente CLI-pull: alguém (ou um cron) tem que chamar `settlement_loader`, `reconcile`, `build-gold` na ordem certa. Não há reação a "arquivo CSV chegou no S3" ou "Kafka topic tem mensagem nova". Uma falha no agendamento externo simplesmente para a cadeia sem alarme próprio.

3. **DuckDB single-process bloqueia concorrência.** Só um processo pode escrever no `warehouse.duckdb` por vez. Rodar `build-gold` enquanto `reconcile` está em progresso resulta em erro de lock. Em produção com múltiplos pipelines paralelos (diferentes `reference_date`s), isso é um gargalo imediato.

---

### O que faria diferente em produção

**O que foi simplificado:**

- **DuckDB local em vez de warehouse distribuído.** Para os ~5M txns/mês do enunciado, DuckDB é suficiente. Para a projeção de 5M/dia (Parte 3.3), seria necessário Spark ou BigQuery com Parquet particionado no S3.

- **Outputs como arquivos estáticos em vez de integrações reais.** O alerta de Ops gera um JSON no formato Slack Block Kit, mas não chama a API do Slack. O relatório do CFO gera HTML, mas não envia email. Em produção: webhook Slack + SMTP/SendGrid com template aprovado pelo time.

- **Orquestração via Makefile + cron externo.** Em produção usaria Airflow ou Dagster para: retry automático, dependência explícita entre tarefas (bronze → silver → gold → outputs), alertas de SLA, e backfill de datas.

- **Sem particionamento no storage.** Bronze e silver vivem num único arquivo DuckDB. Em produção: Parquet particionado por `reference_date` no S3, com o warehouse lendo partições incrementalmente.

---

### Ferramentas de IA utilizadas

**Claude Code (claude-sonnet-4-6)** via CLI para:

- **Design de arquitetura:** discussão das trade-offs entre medallion layers, winning-run policy, por que VIEWs vs TABLEs para cada consumidor.
- **Geração de código:** esqueleto inicial dos loaders bronze, lógica de reconciliação em `reconcile.py`, SQL das views/tables gold, outputs (alerta Ops + relatório CFO HTML).
- **Documentação:** estrutura e conteúdo de `docs/gold-schema.md`, `docs/produtos-de-dados.md`, `docs/arquitetura.md`, `docs/exemplo-queries.md`, e as seções desta entrega.
- **Code review:** verificação de idempotência, gaps de qualidade de dados, e consistência entre o glossário de domínio e a implementação.

O processo foi iterativo: Claude propunha, eu validava contra o enunciado e o glossário, redirecionava onde necessário. As decisões de design (winning-run policy, append-only silver, CFO frozen snapshot) foram deliberadas e discutidas — não defaults do modelo.

---

## Prazo

Você tem **3 a 4 dias** para completar o desafio.

Valorizamos uma solução **completa, limpa e bem documentada** mais do que rica em features. Se o tempo estiver curto, reduza escopo, não qualidade.

---

## Observações

- **Não inclua código malicioso no projeto.** Caso identificado, o projeto será desconsiderado.
- Após a entrega, faremos uma **conversa técnica de ~45 minutos** sobre sua implementação e decisões.

*Boa sorte! Qualquer dúvida sobre o enunciado, entre em contato antes de assumir premissas.*
