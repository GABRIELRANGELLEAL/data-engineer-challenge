"""
Silver seeder for enterprise_company (merchant master data).

Applies CDC (latest-wins by _timestamp, excludes Op='D') and loads the result
into silver_enterprise_company, which is consumed by the gold layer for enrichment.
"""
import logging
import sys
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS silver_enterprise_company (
    id           BIGINT PRIMARY KEY,
    merchant_id  VARCHAR NOT NULL,
    legal_name   VARCHAR,
    trade_name   VARCHAR,
    document     VARCHAR,
    primary_cnae VARCHAR,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ
)
"""


def seed(
    parquet_path: str | Path,
    force: bool = False,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """
    Loads enterprise_company.parquet into silver_enterprise_company.

    Applies CDC: latest row per id (by _timestamp DESC), deletes excluded.

    Args:
        parquet_path: Path to enterprise_company.parquet.
        force:        Drop existing rows and re-seed if the table already has data.

    Returns:
        Number of rows loaded.
    """
    path = _to_posix(parquet_path)

    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        _conn.execute(_CREATE_TABLE)

        existing = _conn.execute(
            "SELECT COUNT(*) FROM silver_enterprise_company"
        ).fetchone()[0]

        if existing > 0 and not force:
            raise RuntimeError(
                f"silver_enterprise_company already has {existing} rows. "
                "Pass force=True to drop and re-seed."
            )
        if force and existing > 0:
            logger.warning("--force: truncating silver_enterprise_company.")
            _conn.execute("DELETE FROM silver_enterprise_company")

        _conn.execute(f"""
            INSERT INTO silver_enterprise_company
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC) AS _rn
                FROM read_parquet('{path}')
            )
            SELECT
                id,
                merchant_id,
                legal_name,
                trade_name,
                document,
                primary_cnae,
                CAST(created_at AS TIMESTAMPTZ),
                CAST(updated_at AS TIMESTAMPTZ)
            FROM ranked
            WHERE _rn = 1 AND Op != 'D'
        """)

        count: int = _conn.execute(
            "SELECT COUNT(*) FROM silver_enterprise_company"
        ).fetchone()[0]
        logger.info("Loaded %d merchants into silver_enterprise_company.", count)
        return count

    finally:
        if owns_conn:
            _conn.close()


def _to_posix(path: str | Path) -> str:
    return str(Path(path).resolve()).replace("\\", "/")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Seed silver_enterprise_company from parquet.")
    parser.add_argument("parquet", help="Path to enterprise_company.parquet")
    parser.add_argument("--force", action="store_true", help="Drop and re-seed if table already has rows")
    args = parser.parse_args()

    try:
        n = seed(args.parquet, force=args.force)
        print(f"Loaded {n} merchants.")
    except Exception as exc:
        logger.error("Seed failed: %s", exc)
        sys.exit(1)
