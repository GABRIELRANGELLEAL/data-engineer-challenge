"""
Silver seeder for enterprise_company (merchant master data).

Applies CDC (latest-wins by _timestamp, excludes Op='D') on top of the bronze
raw extract (raw_enterprise_company) and (re)builds silver_enterprise_company
from scratch. Idempotent — safe to run as many times as needed; each run
fully replaces the table based on whatever is currently in bronze.
"""
import logging
import sys

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_REQUIRED_RAW_TABLE = "raw_enterprise_company"


def seed(conn: duckdb.DuckDBPyConnection | None = None) -> int:
    """
    (Re)builds silver_enterprise_company from raw_enterprise_company (bronze).

    Applies CDC: latest row per id (by _timestamp DESC), deletes excluded.

    Requires src.a_bronze.enterprise_company.load to have already populated
    the raw table.

    Returns:
        Number of rows loaded.
    """
    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        _assert_raw_table_exists(_conn)

        _conn.execute("""
            CREATE OR REPLACE TABLE silver_enterprise_company AS
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC) AS _rn
                FROM raw_enterprise_company
            )
            SELECT
                id,
                merchant_id,
                legal_name,
                trade_name,
                document,
                primary_cnae,
                CAST(created_at AS TIMESTAMPTZ) AS created_at,
                CAST(updated_at AS TIMESTAMPTZ) AS updated_at
            FROM ranked
            WHERE _rn = 1 AND Op != 'D'
        """)
        _conn.execute("ALTER TABLE silver_enterprise_company ADD PRIMARY KEY (id)")

        count: int = _conn.execute(
            "SELECT COUNT(*) FROM silver_enterprise_company"
        ).fetchone()[0]
        logger.info("Loaded %d merchants into silver_enterprise_company.", count)
        return count

    finally:
        if owns_conn:
            _conn.close()


def _assert_raw_table_exists(conn: duckdb.DuckDBPyConnection) -> None:
    exists: bool = conn.execute(
        "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = ?",
        [_REQUIRED_RAW_TABLE],
    ).fetchone()[0]
    if not exists:
        raise RuntimeError(
            f"{_REQUIRED_RAW_TABLE} not found. Run src.a_bronze.enterprise_company "
            "before seeding silver."
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    try:
        n = seed()
        print(f"Loaded {n} merchants.")
    except Exception as exc:
        logger.error("Seed failed: %s", exc)
        sys.exit(1)
