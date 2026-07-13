import os
from pathlib import Path

import duckdb

_DEFAULT_DB_PATH = Path("data/warehouse.duckdb")


def get_connection(db_path: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    path = Path(db_path or os.environ.get("DB_PATH", _DEFAULT_DB_PATH))
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))
