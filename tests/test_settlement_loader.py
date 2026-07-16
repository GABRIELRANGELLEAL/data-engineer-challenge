"""Tests for src/a_bronze/settlement_loader.py.

Focused on the things that can silently corrupt raw_paysettler_settlements:
duplicate primary keys, quality-check bypasses, and folder-level batch
loading (missing columns, empty files, reference_date derivation).
"""
from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from src.a_bronze.settlement_loader import load, load_directory

FIELDS = ["transaction_id", "merchant_id", "amount", "currency", "settled_at", "processor_reference", "status"]
HEADER = ",".join(FIELDS)


def _write_csv(path: Path, rows: list[tuple]) -> Path:
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(FIELDS)
        writer.writerows(rows)
    return path


def _row(txn="TXN1", merch="MERCH1", amount="100.00", currency="BRL",
         settled_at="2025-03-15T10:00:00Z", processor_ref="REF1", status="SETTLED") -> tuple:
    return (txn, merch, amount, currency, settled_at, processor_ref, status)


@pytest.fixture()
def conn():
    c = duckdb.connect()
    yield c
    c.close()


def _pk_duplicates(conn: duckdb.DuckDBPyConnection) -> list:
    return conn.execute("""
        SELECT transaction_id, reference_date, COUNT(*) AS cnt
        FROM raw_paysettler_settlements
        GROUP BY 1, 2
        HAVING cnt > 1
    """).fetchall()


# --- primary key / duplicate integrity -------------------------------------

def test_load_directory_no_duplicate_primary_keys(tmp_path: Path, conn) -> None:
    folder = tmp_path / "paysettler"
    folder.mkdir()
    _write_csv(folder / "settlement_2025-03-15.csv", [_row(txn="TXN1"), _row(txn="TXN2")])
    _write_csv(folder / "settlement_2025-03-16.csv", [_row(txn="TXN1"), _row(txn="TXN3")])

    load_directory(folder, r"(\d{4}-\d{2}-\d{2})", conn=conn)

    assert _pk_duplicates(conn) == []
    total = conn.execute("SELECT COUNT(*) FROM raw_paysettler_settlements").fetchone()[0]
    assert total == 4  # TXN1 appears once per reference_date — that's a valid distinct PK, not a dup


def test_reloading_same_file_is_idempotent(tmp_path: Path, conn) -> None:
    csv_path = _write_csv(tmp_path / "settlement.csv", [_row(txn="TXN1"), _row(txn="TXN2")])

    load(csv_path, "2025-03-15", conn=conn)
    load(csv_path, "2025-03-15", conn=conn)  # re-run for the same file/date

    assert _pk_duplicates(conn) == []
    total = conn.execute("SELECT COUNT(*) FROM raw_paysettler_settlements").fetchone()[0]
    assert total == 2


def test_intra_file_duplicate_keeps_last_occurrence(tmp_path: Path, conn, caplog) -> None:
    csv_path = _write_csv(tmp_path / "settlement.csv", [
        _row(txn="TXN1", amount="100.00"),
        _row(txn="TXN1", amount="200.00"),  # same transaction_id, different amount
    ])

    with caplog.at_level("WARNING"):
        load(csv_path, "2025-03-15", conn=conn)

    assert any("duplicate transaction_id" in r.message for r in caplog.records)
    rows = conn.execute(
        "SELECT amount FROM raw_paysettler_settlements WHERE transaction_id = 'TXN1'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 200.00


# --- quality checks must halt the load, not silently corrupt the table -----

def test_zero_or_negative_amount_is_rejected(tmp_path: Path, conn) -> None:
    csv_path = _write_csv(tmp_path / "settlement.csv", [_row(txn="TXN1", amount="0.00")])
    with pytest.raises(ValueError, match="Negative or zero amounts"):
        load(csv_path, "2025-03-15", conn=conn)
    assert conn.execute("SELECT COUNT(*) FROM raw_paysettler_settlements").fetchone()[0] == 0


def test_null_transaction_id_is_rejected(tmp_path: Path, conn) -> None:
    csv_path = _write_csv(tmp_path / "settlement.csv", [_row(txn="")])
    with pytest.raises(ValueError, match="transaction_ids"):
        load(csv_path, "2025-03-15", conn=conn)


def test_null_merchant_id_is_rejected(tmp_path: Path, conn) -> None:
    csv_path = _write_csv(tmp_path / "settlement.csv", [_row(merch="")])
    with pytest.raises(ValueError, match="merchant_ids"):
        load(csv_path, "2025-03-15", conn=conn)


def test_amount_normalization_clean_and_brl_formats(tmp_path: Path, conn) -> None:
    csv_path = _write_csv(tmp_path / "settlement.csv", [
        _row(txn="TXN1", amount="152.30"),        # clean dot-decimal (domain spec default)
        _row(txn="TXN2", amount="R$ 32.245,91"),   # BRL notation (comma decimal, dot thousands)
    ])

    load(csv_path, "2025-03-15", conn=conn)

    amounts = dict(conn.execute(
        "SELECT transaction_id, amount FROM raw_paysettler_settlements ORDER BY transaction_id"
    ).fetchall())
    assert float(amounts["TXN1"]) == 152.30
    assert float(amounts["TXN2"]) == 32245.91


# --- folder-level batch loading --------------------------------------------

def test_reference_date_derived_from_filename(tmp_path: Path, conn) -> None:
    folder = tmp_path / "paysettler"
    folder.mkdir()
    _write_csv(folder / "settlement_2025-03-15.csv", [_row(txn="TXN1")])

    load_directory(folder, r"(\d{4}-\d{2}-\d{2})", conn=conn)

    ref_date = conn.execute(
        "SELECT reference_date FROM raw_paysettler_settlements WHERE transaction_id = 'TXN1'"
    ).fetchone()[0]
    assert str(ref_date) == "2025-03-15"


def test_reference_date_pattern_accepts_callable(tmp_path: Path, conn) -> None:
    folder = tmp_path / "paysettler"
    folder.mkdir()
    _write_csv(folder / "day1.csv", [_row(txn="TXN1")])

    results = load_directory(folder, lambda p: "2025-01-01", conn=conn)

    assert results == {"day1.csv": 1}
    ref_date = conn.execute("SELECT reference_date FROM raw_paysettler_settlements").fetchone()[0]
    assert str(ref_date) == "2025-01-01"


def test_file_missing_expected_column_is_skipped_not_loaded(tmp_path: Path, conn) -> None:
    folder = tmp_path / "paysettler"
    folder.mkdir()
    # Missing the 'status' column entirely.
    bad = folder / "settlement_2025-03-15.csv"
    bad.write_text(
        "transaction_id,merchant_id,amount,currency,settled_at,processor_reference\n"
        "TXN1,MERCH1,100.00,BRL,2025-03-15T10:00:00Z,REF1\n"
    )
    _write_csv(folder / "settlement_2025-03-16.csv", [_row(txn="TXN2")])

    results = load_directory(folder, r"(\d{4}-\d{2}-\d{2})", conn=conn)

    assert "settlement_2025-03-15.csv" not in results
    assert results == {"settlement_2025-03-16.csv": 1}
    total = conn.execute("SELECT COUNT(*) FROM raw_paysettler_settlements").fetchone()[0]
    assert total == 1


def test_empty_file_is_skipped_not_loaded(tmp_path: Path, conn) -> None:
    folder = tmp_path / "paysettler"
    folder.mkdir()
    (folder / "settlement_2025-03-15.csv").write_text(HEADER + "\n")  # header only, no rows
    _write_csv(folder / "settlement_2025-03-16.csv", [_row(txn="TXN2")])

    results = load_directory(folder, r"(\d{4}-\d{2}-\d{2})", conn=conn)

    assert "settlement_2025-03-15.csv" not in results
    assert results == {"settlement_2025-03-16.csv": 1}


def test_one_bad_file_does_not_abort_the_whole_batch(tmp_path: Path, conn) -> None:
    folder = tmp_path / "paysettler"
    folder.mkdir()
    _write_csv(folder / "settlement_2025-03-15.csv", [_row(txn="TXN1", amount="-5.00")])  # fails quality check
    _write_csv(folder / "settlement_2025-03-16.csv", [_row(txn="TXN2")])

    results = load_directory(folder, r"(\d{4}-\d{2}-\d{2})", conn=conn)

    assert "settlement_2025-03-15.csv" not in results
    assert results == {"settlement_2025-03-16.csv": 1}
    assert _pk_duplicates(conn) == []


def test_non_csv_files_ignored_by_default_pattern(tmp_path: Path, conn) -> None:
    folder = tmp_path / "paysettler"
    folder.mkdir()
    _write_csv(folder / "settlement_2025-03-15.csv", [_row(txn="TXN1")])
    (folder / "README.txt").write_text("not a csv")

    results = load_directory(folder, r"(\d{4}-\d{2}-\d{2})", conn=conn)

    assert results == {"settlement_2025-03-15.csv": 1}


def test_missing_folder_raises(tmp_path: Path, conn) -> None:
    with pytest.raises(NotADirectoryError):
        load_directory(tmp_path / "does-not-exist", r"(\d{4}-\d{2}-\d{2})", conn=conn)
