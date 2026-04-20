# Glossário do Domínio — Reconciliação de Liquidações

## Visão Geral

A empresa processa pagamentos de merchants (estabelecimentos comerciais) e utiliza um processador externo chamado **PaySettler** para realizar as liquidações. Diariamente, o PaySettler envia um arquivo CSV com as transações liquidadas, e o sistema interno compara esses dados com seus próprios registros — esse processo é a **reconciliação**.

---

## Categorias de Reconciliação

Cada transação é classificada em uma das seguintes categorias:

### Matched

Transação existe tanto no arquivo do processador quanto no sistema interno, e os valores estão de acordo (dentro da tolerância aceitável).

### Mismatched

Transação existe em ambos os lados, mas o valor do processador difere do valor interno **além da tolerância aceitável**. Essas discrepâncias precisam ser investigadas pelo time de operações.

### Unreconciled (processor)

Transação presente no arquivo do processador, mas que **não existe** nos registros internos. Pode indicar uma transação que não foi registrada internamente, ou um erro do processador.

### Unreconciled (internal)

Transação presente nos registros internos que **não apareceu** no arquivo do processador dentro do escopo temporal da reconciliação. Pode indicar que a liquidação ainda não ocorreu, ou que houve uma falha no processamento.

---

## Regras de Negócio

### Tolerância

Diferenças de até **R$ 0,01** (um centavo) entre o valor do processador e o valor interno são consideradas aceitáveis e não configuram mismatch. Isso se deve a arredondamentos que ocorrem no processamento de pagamentos.

### Escopo Temporal

A reconciliação compara o arquivo do processador com transações internas criadas em uma **janela de 7 dias** ancorada em uma **data de referência** (`reference_date`).

**Data de referência (`reference_date`):**
- É a data à qual o arquivo CSV se refere — o dia de liquidação que o arquivo representa
- Formato: `YYYY-MM-DD`, interpretada em UTC
- Persistida em cada `reconciliation_run`

**Janela de 7 dias:**
- Intervalo considerado: `[reference_date - 7 dias, reference_date]` (inclusivo nas duas pontas)
- Aplicada sobre o `created_at` das transações internas

### Matching

A correspondência entre uma transação do processador e uma transação interna é feita pelo campo `transaction_id`. Ambos os sistemas usam o mesmo identificador UUID.

---

## Reconciliation Run

Cada execução do processo de reconciliação é um **reconciliation run**. Um run contém:

- Identificador único (`id`)
- Data de referência (`reference_date`) — tempo de negócio
- Timestamps de início e fim — tempo de sistema
- Nome do arquivo processado
- Status: `IN_PROGRESS`, `COMPLETED`, `FAILED`
- Total de transações no arquivo
- Lista de resultados categorizados (`reconciliation_results`)

---

## Arquivo CSV do PaySettler

### Formato

- **Encoding:** UTF-8
- **Separador:** Vírgula (`,`)
- **Header:** Primeira linha contém nomes das colunas
- **Campos com vírgula:** Envolvidos em aspas duplas (`"`)

### Colunas

| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| `transaction_id` | UUID | Identificador da transação | `550e8400-e29b-41d4-a716-446655440000` |
| `merchant_id` | String | Identificador do merchant | `MERCH_001` |
| `amount` | Decimal | Valor liquidado (ponto como separador) | `152.30` |
| `currency` | String (ISO 4217) | Moeda | `BRL` |
| `settled_at` | DateTime (ISO 8601, UTC) | Data/hora da liquidação | `2025-03-15T14:30:00Z` |
| `processor_reference` | String | Referência interna do processador | `PS-2025-00012345` |
| `status` | String | Status da liquidação | `SETTLED` |

### Status do PaySettler

- `SETTLED` — Transação liquidada normalmente
- `REVERSED` — Transação revertida após liquidação (chargeback, estorno)
