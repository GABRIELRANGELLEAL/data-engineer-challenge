cd /caminho/do/seu/repo

### Sobe o container
```bash
docker compose up -d --build
```

### Popula silver
```bash
make seed-silver
make seed-company
```

### Constrói a gold layer
```bash
make build-gold
```

### Shell DuckDB interativo dentro do container:

```bash
docker compose exec pipeline python -m duckdb data/warehouse.duckdb
```
### shell Python interativo dentro do container
```bash
docker compose exec pipeline python
```
```python
import duckdb
conn = duckdb.connect('data/warehouse.duckdb')
```
```python
conn.sql("SHOW TABLES").show()
conn.sql("SELECT COUNT(*) FROM silver_reconciliation_runs").show()
conn.sql("SELECT COUNT(*) FROM silver_reconciliation_results").show()

conn.sql("""
    SELECT id, reference_date, status, total_transactions, started_at, completed_at
    FROM silver_reconciliation_runs
    ORDER BY reference_date, started_at
""").show()

conn.sql("""
    SELECT category, COUNT(*) AS n
    FROM silver_reconciliation_results
    GROUP BY category
    ORDER BY n DESC
""").show()

conn.sql("""
    SELECT
        category,
        COUNT(*) AS n,
        COUNT(internal_amount) AS n_with_internal,
        COUNT(processor_amount) AS n_with_processor
    FROM silver_reconciliation_results
    GROUP BY category
""").show()
```