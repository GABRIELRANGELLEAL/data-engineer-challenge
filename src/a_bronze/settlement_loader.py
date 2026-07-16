import logging
import re
from pathlib import Path
from typing import Callable

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw_paysettler_settlements (
    transaction_id      VARCHAR NOT NULL,
    reference_date      DATE NOT NULL,
    merchant_id         VARCHAR NOT NULL,
    amount              DECIMAL(18, 2) NOT NULL,
    currency            VARCHAR(3) NOT NULL,
    settled_at          TIMESTAMPTZ NOT NULL,
    processor_reference VARCHAR NOT NULL,
    status              VARCHAR NOT NULL CHECK (status IN ('SETTLED', 'REVERSED')),
    _loaded_at          TIMESTAMP DEFAULT current_timestamp,
    _source_file        VARCHAR NOT NULL,
    PRIMARY KEY (transaction_id, reference_date)
)
"""

# The domain spec says amounts use a decimal point (e.g. 152.30), but ~4% of
# rows in real data use BRL notation with a comma decimal separator and '.'
# as thousands grouping (e.g. "R$ 32.245,91"). A comma is only ever present
# in the BRL form, so it's used to pick which parsing rule applies — treating
# '.' as a thousands separator unconditionally (the previous approach) corrupts
# the clean dot-decimal format by stripping its decimal point.
_NORMALIZE_AMOUNT = r"""
    CASE
        WHEN TRIM(amount) LIKE '%,%' THEN
            CAST(
                REPLACE(
                    REPLACE(
                        REGEXP_REPLACE(TRIM(amount), '^R\$\s*', ''),
                    '.', ''),
                ',', '.')
            AS DECIMAL(18, 2))
        ELSE
            CAST(REGEXP_REPLACE(TRIM(amount), '^R\$\s*', '') AS DECIMAL(18, 2))
    END
"""

# Force `amount` to be read as VARCHAR regardless of file content — otherwise
# DuckDB's type inference may pick DOUBLE/DECIMAL for "clean" files (no R$ or
# comma-decimal rows), which breaks TRIM()/REGEXP_REPLACE() in _NORMALIZE_AMOUNT.
# parallel=false forces a sequential scan so row order matches file order —
# required for _deduped_csv's "last occurrence wins" tie-break below.
def _read_csv(csv_str: str) -> str:
    return f"read_csv_auto('{csv_str}', header=true, types={{'amount': 'VARCHAR'}}, parallel=false)"


# Collapses intra-file duplicate transaction_ids, keeping the last occurrence
# in file order — matches the warning logged by _check_intra_file_duplicates.
# Without this, INSERT OR REPLACE's own conflict resolution silently keeps the
# *first* occurrence when duplicate keys appear in a single INSERT statement.
def _deduped_csv(csv_str: str) -> str:
    return f"""(
        WITH ordered AS (
            SELECT *, ROW_NUMBER() OVER () AS _file_rn
            FROM {_read_csv(csv_str)}
        )
        SELECT * EXCLUDE (_file_rn)
        FROM ordered
        QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _file_rn DESC) = 1
    )"""


_EXPECTED_COLUMNS = {
    "transaction_id",
    "merchant_id",
    "amount",
    "currency",
    "settled_at",
    "processor_reference",
    "status",
}


_QUALITY_CHECKS = [
    (
        f"({_NORMALIZE_AMOUNT}) > 0",
        "Negative or zero amounts found",
        True,
    ),
    (
        "transaction_id IS NOT NULL AND LENGTH(TRIM(transaction_id)) > 0",
        "NULL or empty transaction_ids found",
        True,
    ),
    (
        "merchant_id IS NOT NULL AND LENGTH(TRIM(merchant_id)) > 0",
        "NULL or empty merchant_ids found",
        True,
    ),
]


def load(
    csv_path: str | Path,
    reference_date: str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """
    Loads a PaySettler CSV into raw_paysettler_settlements.

    Append-only per (transaction_id, reference_date): re-running for the same
    file replaces existing rows for that key but does not touch other dates.

    Args:
        csv_path:       Path to the PaySettler CSV file.
        reference_date: Business date the CSV represents (YYYY-MM-DD).
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Settlement file not found: {csv_path}")

    csv_str = str(csv_path.resolve()).replace("\\", "/")

    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        _conn.execute(_CREATE_TABLE)

        _check_intra_file_duplicates(_conn, csv_str, csv_path.name)
        _run_quality_checks(_conn, csv_str)

        row_count: int = _conn.execute(
            f"SELECT COUNT(*) FROM {_deduped_csv(csv_str)}"
        ).fetchone()[0]

        _conn.execute(f"""
            INSERT OR REPLACE INTO raw_paysettler_settlements
            SELECT
                transaction_id,
                CAST('{reference_date}' AS DATE) AS reference_date,
                merchant_id,
                {_NORMALIZE_AMOUNT},
                currency,
                CAST(settled_at AS TIMESTAMPTZ),
                processor_reference,
                status,
                current_timestamp AS _loaded_at,
                '{csv_path.name}' AS _source_file
            FROM {_deduped_csv(csv_str)}
        """)

        logger.info(
            "Loaded %d rows from %s (reference_date=%s) into raw_paysettler_settlements",
            row_count,
            csv_path.name,
            reference_date,
        )
        return row_count

    finally:
        if owns_conn:
            _conn.close()


def load_directory(
    folder_path: str | Path,
    reference_date_pattern: str | Callable[[Path], str],
    conn: duckdb.DuckDBPyConnection | None = None,
    pattern: str = "*.csv",
) -> dict[str, int]:
    """
    Batch-loads every PaySettler CSV in a folder into raw_paysettler_settlements,
    reusing load() per file.

    Args:
        folder_path:            Directory containing the CSV files.
        reference_date_pattern: Either a callable that takes the file's Path and
                                 returns its reference_date (YYYY-MM-DD), or a
                                 regex string applied to the filename — the first
                                 capture group (or the whole match, if the regex
                                 has no groups) is used as the reference_date.
        conn:                   Optional existing DuckDB connection to reuse.
        pattern:                Glob pattern used to select files (default "*.csv").

    Returns:
        Mapping of filename to the number of rows loaded for that file. Files
        that fail to load are logged and omitted from the result rather than
        aborting the whole batch.
    """
    folder = Path(folder_path)
    if not folder.is_dir():
        raise NotADirectoryError(f"Settlement folder not found: {folder}")

    csv_paths = sorted(folder.glob(pattern))
    if not csv_paths:
        logger.warning("No files matching '%s' found in %s", pattern, folder)

    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    results: dict[str, int] = {}
    try:
        valid_paths: list[Path] = []
        for csv_path in csv_paths:
            try:
                _validate_csv_file(_conn, csv_path)
                valid_paths.append(csv_path)
            except Exception as exc:
                logger.error("Pre-load validation failed for %s: %s", csv_path.name, exc)

        for csv_path in valid_paths:
            try:
                reference_date = _derive_reference_date(csv_path, reference_date_pattern)
                results[csv_path.name] = load(csv_path, reference_date, conn=_conn)
            except Exception:
                logger.exception("Failed to load %s — skipping", csv_path.name)

        logger.info(
            "Loaded %d/%d file(s) from %s into raw_paysettler_settlements",
            len(results),
            len(csv_paths),
            folder,
        )
        return results

    finally:
        if owns_conn:
            _conn.close()


def _derive_reference_date(
    csv_path: Path,
    reference_date_pattern: str | Callable[[Path], str],
) -> str:
    if callable(reference_date_pattern):
        return reference_date_pattern(csv_path)

    match = re.search(reference_date_pattern, csv_path.name)
    if not match:
        raise ValueError(
            f"Could not derive reference_date from '{csv_path.name}' "
            f"using pattern '{reference_date_pattern}'"
        )
    return match.group(1) if match.groups() else match.group(0)


def _validate_csv_file(conn: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    """
    Pre-flight checks run before a file enters the load loop: header must
    contain all expected columns, and the file must have at least one data row.
    Raises ValueError on failure; callers decide whether to skip or halt.
    """
    csv_str = str(csv_path.resolve()).replace("\\", "/")

    columns = {col[0] for col in conn.execute(f"DESCRIBE SELECT * FROM {_read_csv(csv_str)}").fetchall()}
    missing = _EXPECTED_COLUMNS - columns
    if missing:
        raise ValueError(f"Missing expected column(s) {sorted(missing)} (found: {sorted(columns)})")

    row_count: int = conn.execute(f"SELECT COUNT(*) FROM {_read_csv(csv_str)}").fetchone()[0]
    if row_count == 0:
        raise ValueError("File contains no data rows")


def _check_intra_file_duplicates(
    conn: duckdb.DuckDBPyConnection,
    csv_str: str,
    filename: str,
) -> None:
    dups = conn.execute(f"""
        SELECT transaction_id, COUNT(*) AS cnt
        FROM {_read_csv(csv_str)}
        GROUP BY transaction_id
        HAVING cnt > 1
    """).fetchall()

    if dups:
        logger.warning(
            "File %s contains %d duplicate transaction_id(s) — last occurrence kept: %s",
            filename,
            len(dups),
            [d[0] for d in dups],
        )


def _run_quality_checks(conn: duckdb.DuckDBPyConnection, csv_str: str) -> None:
    for condition, message, halt in _QUALITY_CHECKS:
        violations: int = conn.execute(f"""
            SELECT COUNT(*)
            FROM {_read_csv(csv_str)}
            WHERE NOT ({condition})
        """).fetchone()[0]

        if violations > 0:
            if halt:
                raise ValueError(f"Quality check failed — {message} ({violations} rows)")
            logger.warning("Quality check warning — %s (%d rows)", message, violations)