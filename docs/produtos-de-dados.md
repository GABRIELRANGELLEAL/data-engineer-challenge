# Produtos de Dados — Contexto por Consumidor

> **Referência técnica** (schema, decisões de design, idempotência) está em `docs/gold-schema.md`.
> Este documento cobre o *porquê* de cada artefato: qual consumidor usa, que perguntas responde, o que deliberadamente fica de fora, e por que priorizamos o que priorizamos.

---

## Operações

**Artefatos:**
 - `gold_ops_reconciliation_daily` (VIEW), 
 - `gold_ops_reconciliation_trend` (VIEW)

### Perguntas que o modelo responde

| Pergunta | Como responder |
|----------|----------------|
| Qual a taxa de MISMATCHED hoje? | `pct_of_total WHERE category = 'MISMATCHED'` em `gold_ops_reconciliation_daily` |
| A taxa de UNRECONCILED está subindo? | `pct_of_total` vs `pct_of_total_7d_avg` em `gold_ops_reconciliation_trend` |
| Qual o volume financeiro em discrepância hoje? | `abs_difference_sum WHERE category = 'MISMATCHED'` |
| Quantas transações ficaram sem contraparte no processador hoje? | `txn_count WHERE category = 'UNRECONCILED_PROCESSOR'` |
| Em quantos dias dos últimos 7 a taxa de MISMATCHED ficou acima de X%? | filtro sobre `gold_ops_reconciliation_trend` |


### Por que essas e não outras

Ops precisa de **sinal de saúde em tempo real** para agir hoje, não de relatórios retrospectivos. O design escolheu `VIEW` (não table materializada) justamente para garantir que o dado reflita sempre o estado mais recente da silver layer, sem delay de rebuild. A adição da média móvel de 7 dias (`pct_of_total_7d_avg`) no trend resolve o problema de "subindo ou é flutuação normal?"

---

## CFO

**Artefatos:** `gold_cfo_weekly_summary` (TABLE), `gold_cfo_weekly_merchant_ranking` (TABLE)

### Perguntas que o modelo responde

| Pergunta | Como responder |
|----------|----------------|
| Qual o volume total transacionado (em BRL) esta semana? | `SUM(amount_brl)` em `gold_cfo_weekly_summary` para a semana corrente |
| Quanto veio de transações MATCHED vs não-reconciliadas? | `amount_brl GROUP BY category` em `gold_cfo_weekly_summary` |
| Esta semana o volume foi maior ou menor que a anterior? | comparar `week_start` e `week_start - 7 days` em `gold_cfo_weekly_summary` |
| Quais merchants concentram o maior valor em discrepância esta semana? | `gold_cfo_weekly_merchant_ranking ORDER BY amount_brl DESC` |
| Qual merchant específico aparece mais nas categorias de risco? | filtrar `merchant_id` + `category IN ('MISMATCHED', 'UNRECONCILED_*')` |

### Perguntas que o modelo deliberadamente **não** responde

- **Projeção de volume futuro:** o modelo é histórico puro; projeção exigiria uma camada de modelagem preditiva.
- **Breakdown por CNAE / segmento:** `primary_cnae` está em `silver_enterprise_company` mas não foi incluído — adicioná-lo aumenta a granularidade do ranking e é um incremento natural se o CFO precisar.
- **Comparação com meta orçada:** o modelo não conhece targets; isso exigiria integração com o sistema financeiro.
- **Visão diária** para o CFO: a granularidade semanal foi uma decisão de escopo — suficiente para relatório executivo e coerente com o ciclo de gestão.

### Por que essas e não outras

O CFO precisa de um **número que não mude depois que saiu do relatório**. Por isso `gold_cfo_weekly_summary` é uma `TABLE` materializada — se um date passado for reprocessado, a tabela não muda retroativamente (o snapshot da semana já foi "fechado"). A convenção `COALESCE(processor_amount, internal_amount)` garante que `amount_brl` nunca seja NULL e usa sempre o valor mais externamante confirmado disponível.

O merchant ranking inclui apenas categorias de risco (não MATCHED) porque MATCHED não tem relevância operacional para o CFO — ele quer saber onde está o problema, não confirmar o que funcionou.

---

## Compliance

**Artefato:** `gold_compliance_ledger` (VIEW)

### Perguntas que o modelo responde

| Pergunta | Como responder |
|----------|----------------|
| O que o pipeline reportou para a transação X em tal data? | `WHERE transaction_id = 'X' AND reference_date = 'YYYY-MM-DD'` |
| Quantas vezes a data D foi reconciliada e qual o resultado de cada run? | `WHERE reference_date = 'D' ORDER BY run_id` |
| Quais transações de um merchant específico ficaram MISMATCHED em determinado período? | `WHERE merchant_id = 'M' AND category = 'MISMATCHED'` + filtro de data |
| Qual o CNPJ e razão social do merchant associado a uma transação? | colunas `legal_name`, `document` (join com `silver_enterprise_company` embutido na view) |
| O pipeline estava rodando corretamente quando a transação X foi processada? | colunas `started_at`, `completed_at`, `run_status` do run associado |
| Houve reruns para a data D? Qual run está vigente? | múltiplas linhas para o mesmo `transaction_id + reference_date` revelam reruns |

### Perguntas que o modelo deliberadamente **não** responde

- **Quem aprovou uma reversão ou estorno:** esse dado não existe nas fontes disponíveis (não há sistema de autorização integrado).
- **Histórico de mudanças em um merchant:** `silver_enterprise_company` armazena o estado atual; histórico de alterações cadastrais exigiria uma tabela de versionamento separada.
- **Apenas o run vigente:** a view mostra *todos* os runs intencionalmente. Compliance precisa ver o histórico completo para responder "o que o sistema sabia em dado momento". Filtrar pelo winning-run seria um bug silencioso de auditoria.

### Por que essas e não outras

Compliance tem um requisito único: **imutabilidade e completude**. A silver layer é append-only por design — nenhuma linha de resultado é jamais atualizada ou deletada. A view de compliance expõe essa característica diretamente, sem filtro de winning-run, para que auditores possam reconstruir o estado exato do sistema em qualquer ponto no tempo. O uso de `VIEW` (não table materializada) garante que qualquer novo run adicionado aparece automaticamente nas queries de auditoria sem rebuild.
