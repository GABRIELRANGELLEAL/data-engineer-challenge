# Exemplos de Queries — Gold Layer

> Todas as queries rodam contra o DuckDB local (`data/warehouse.duckdb`) após executar `make build-gold`.
> Para as perguntas de negócio que cada query responde, veja `docs/produtos-de-dados.md`.

---

## Operações

### 1. Taxa de discrepância hoje (última `reference_date` processada)

```sql
SELECT
    reference_date,
    category,
    txn_count,
    ROUND(pct_of_total * 100, 2)  AS pct,
    abs_difference_sum
FROM gold_ops_reconciliation_daily
WHERE reference_date = (SELECT MAX(reference_date) FROM gold_ops_reconciliation_daily)
ORDER BY category;
```

**Responde:** qual a distribuição de categorias no dia mais recente e o volume financeiro em discrepância.

---

### 2. Detecção de anomalia — MISMATCHED acima da média dos últimos 7 dias

```sql
SELECT
    reference_date,
    ROUND(pct_of_total * 100, 2)      AS pct_today,
    ROUND(pct_of_total_7d_avg * 100, 2) AS pct_7d_avg,
    ROUND((pct_of_total - pct_of_total_7d_avg) * 100, 2) AS delta_pp
FROM gold_ops_reconciliation_trend
WHERE category = 'MISMATCHED'
ORDER BY reference_date DESC
LIMIT 14;
```

**Responde:** a taxa de MISMATCHED está subindo em relação ao padrão recente? `delta_pp > 0` indica piora.

---

### 3. Evolução semanal da taxa de UNRECONCILED (últimas 4 semanas)

```sql
SELECT
    DATE_TRUNC('week', reference_date)::DATE AS week_start,
    category,
    SUM(txn_count)                            AS txn_count,
    ROUND(AVG(pct_of_total) * 100, 2)         AS avg_daily_pct
FROM gold_ops_reconciliation_daily
WHERE category IN ('UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')
  AND reference_date >= CURRENT_DATE - INTERVAL '28 days'
GROUP BY week_start, category
ORDER BY week_start DESC, category;
```

**Responde:** a taxa de não-reconciliação está subindo semana a semana?

---

## CFO

### 4. Volume financeiro semanal por categoria (últimas 8 semanas)

```sql
SELECT
    week_start,
    week_end,
    category,
    txn_count,
    ROUND(amount_brl, 2) AS amount_brl
FROM gold_cfo_weekly_summary
ORDER BY week_start DESC, category;
```

**Responde:** qual o volume total transacionado por semana, separado por categoria de reconciliação.

---

### 5. Comparação semana atual vs semana anterior

```sql
WITH latest_week AS (
    SELECT MAX(week_start) AS w FROM gold_cfo_weekly_summary
),
current_week AS (
    SELECT category, SUM(amount_brl) AS amount, SUM(txn_count) AS txns
    FROM gold_cfo_weekly_summary
    WHERE week_start = (SELECT w FROM latest_week)
    GROUP BY category
),
prior_week AS (
    SELECT category, SUM(amount_brl) AS amount, SUM(txn_count) AS txns
    FROM gold_cfo_weekly_summary
    WHERE week_start = (SELECT w - INTERVAL '7 days' FROM latest_week)
    GROUP BY category
)
SELECT
    COALESCE(c.category, p.category) AS category,
    ROUND(c.amount, 2)               AS current_week_brl,
    ROUND(p.amount, 2)               AS prior_week_brl,
    ROUND(c.amount - p.amount, 2)    AS delta_brl,
    c.txns                           AS current_week_txns,
    p.txns                           AS prior_week_txns
FROM current_week c
FULL JOIN prior_week p USING (category)
ORDER BY current_week_brl DESC NULLS LAST;
```

**Responde:** o volume desta semana cresceu ou caiu em relação à anterior, por categoria.

---

### 6. Top 10 merchants por valor em risco (semana mais recente)

```sql
SELECT
    merchant_id,
    COALESCE(trade_name, legal_name, merchant_id) AS merchant,
    category,
    txn_count,
    ROUND(amount_brl, 2) AS amount_brl
FROM gold_cfo_weekly_merchant_ranking
WHERE week_start = (SELECT MAX(week_start) FROM gold_cfo_weekly_merchant_ranking)
ORDER BY amount_brl DESC
LIMIT 10;
```

**Responde:** quais merchants concentram o maior valor financeiro nas categorias de risco nesta semana.

---

## Compliance

### 7. Trilha de auditoria completa de uma transação específica

```sql
SELECT
    result_id,
    run_id,
    reference_date,
    file_name,
    run_status,
    started_at,
    completed_at,
    category,
    internal_amount,
    processor_amount,
    difference,
    merchant_id,
    legal_name,
    document
FROM gold_compliance_ledger
WHERE transaction_id = '<uuid-da-transacao>'
ORDER BY run_id;
```

**Responde:** o histórico completo de todos os runs que processaram esta transação — incluindo reruns e mudanças de categoria entre runs.

---

### 8. Todas as reconciliações de uma data de referência (detecção de reruns)

```sql
SELECT
    run_id,
    reference_date,
    file_name,
    run_status,
    started_at,
    completed_at,
    COUNT(*) AS result_count
FROM gold_compliance_ledger
WHERE reference_date = '2025-03-15'
GROUP BY run_id, reference_date, file_name, run_status, started_at, completed_at
ORDER BY run_id;
```

**Responde:** quantas vezes uma data foi reconciliada, qual o status de cada run e quando ocorreu. Múltiplas linhas indicam reruns.

---

### 9. Transações MISMATCHED de um merchant em um período para auditoria

```sql
SELECT
    result_id,
    run_id,
    reference_date,
    transaction_id,
    internal_amount,
    processor_amount,
    difference,
    legal_name,
    document,
    created_at
FROM gold_compliance_ledger
WHERE merchant_id     = 'MERCH_001'
  AND category        = 'MISMATCHED'
  AND reference_date BETWEEN '2025-03-01' AND '2025-03-31'
  AND run_status      = 'COMPLETED'
ORDER BY reference_date, difference DESC;
```

**Responde:** lista auditável de todas as discrepâncias de um merchant em um período, com valores de ambos os lados e o delta.
