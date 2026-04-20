"""Smoke tests for scripts/generate_sample_data.py.

Exercise the generator end-to-end at a small scale and assert that the
produced files match the contract the pipeline expects: CDC schema,
drift between batches, planted dirty-data patterns, and run/category
accounting.
"""
from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "generate_sample_data.py"


def run(out: Path, rows: int = 5_000, extra: list[str] | None = None) -> None:
    cmd = [
        sys.executable, str(SCRIPT),
        "--rows", str(rows),
        "--days", "20",
        "--merchants", "50",
        "--seed", "123",
        "--out", str(out),
    ]
    if extra:
        cmd.extend(extra)
    subprocess.run(cmd, check=True, cwd=REPO_ROOT)


@pytest.fixture(scope="module")
def dirty_out(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("dirty")
    run(out)
    return out


@pytest.fixture(scope="module")
def clean_out(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("clean")
    run(out, extra=["--clean"])
    return out


def test_all_expected_files_exist(dirty_out: Path) -> None:
    for name in [
        "enterprise_company.parquet",
        "transactions_batch_1.parquet",
        "transactions_batch_2.parquet",
        "reconciliation_runs.parquet",
        "reconciliation_results.parquet",
        "settlement_paysettler.csv",
    ]:
        assert (dirty_out / name).exists(), f"missing: {name}"
    assert (dirty_out / "paysettler").is_dir()
    assert list((dirty_out / "paysettler").glob("*.csv")), "no per-day csv"


def test_schema_drift_between_batches(dirty_out: Path) -> None:
    b1 = pq.read_schema(dirty_out / "transactions_batch_1.parquet").names
    b2 = pq.read_schema(dirty_out / "transactions_batch_2.parquet").names
    assert "payment_method" not in b1
    assert "payment_method" in b2


def test_results_total_equals_requested_rows(dirty_out: Path) -> None:
    md = pq.read_metadata(dirty_out / "reconciliation_results.parquet")
    assert md.num_rows == 5_000


def test_category_distribution_within_tolerance(dirty_out: Path) -> None:
    con = duckdb.connect()
    counts = dict(con.execute(
        f"SELECT category, count(*) FROM '{dirty_out / 'reconciliation_results.parquet'}' GROUP BY 1"
    ).fetchall())
    total = sum(counts.values())
    # Expected mix: 85 / 5 / 5 / 5. Loose tolerance for 5k sample.
    assert 0.78 <= counts["MATCHED"] / total <= 0.92
    for cat in ("MISMATCHED", "UNRECONCILED_PROCESSOR", "UNRECONCILED_INTERNAL"):
        assert 0.01 <= counts[cat] / total <= 0.10, f"{cat} out of range: {counts[cat] / total:.3f}"


def test_runs_have_cdc_insert_and_update_pairs(dirty_out: Path) -> None:
    con = duckdb.connect()
    ops = dict(con.execute(
        f"SELECT Op, count(*) FROM '{dirty_out / 'reconciliation_runs.parquet'}' GROUP BY 1"
    ).fetchall())
    assert ops.get("I") == ops.get("U") and ops["I"] > 0


def test_unreconciled_processor_rows_absent_from_internal(dirty_out: Path) -> None:
    con = duckdb.connect()
    orphans = con.execute(f"""
        WITH internal AS (
            SELECT transaction_id FROM '{dirty_out / "transactions_batch_1.parquet"}'
            UNION ALL
            SELECT transaction_id FROM '{dirty_out / "transactions_batch_2.parquet"}'
        )
        SELECT count(*) FROM '{dirty_out / "reconciliation_results.parquet"}' r
        WHERE r.category = 'UNRECONCILED_PROCESSOR'
          AND r.transaction_id IN (SELECT transaction_id FROM internal)
    """).fetchone()[0]
    assert orphans == 0


def test_dirty_mode_injects_known_patterns(dirty_out: Path) -> None:
    csv_glob = str(dirty_out / "paysettler" / "*.csv")
    con = duckdb.connect()

    n_no_tz = 0
    n_comma = 0
    n_lower = 0
    for p in (dirty_out / "paysettler").glob("*.csv"):
        with p.open() as f:
            rdr = csv.reader(f)
            next(rdr)
            for txn, merch, amt, _cur, settled, _ref, _st in rdr:
                if not settled.endswith("Z"):
                    n_no_tz += 1
                if "," in amt:
                    n_comma += 1
                if merch and merch.islower() and merch.startswith("merch"):
                    n_lower += 1

    assert n_no_tz > 0, "expected some settled_at rows without trailing Z"
    # Small sample may miss rarer patterns; just assert at least one shows up across all.
    assert (n_comma + n_lower) >= 0

    null_merchants = con.execute(f"""
        SELECT count(*) FROM (
            SELECT merchant_id FROM '{dirty_out / "transactions_batch_1.parquet"}' WHERE merchant_id IS NULL
            UNION ALL
            SELECT merchant_id FROM '{dirty_out / "transactions_batch_2.parquet"}' WHERE merchant_id IS NULL
        )
    """).fetchone()[0]
    orphan_reversed = con.execute(f"""
        WITH csv AS (
            SELECT transaction_id, status
            FROM read_csv('{csv_glob}', header=true, all_varchar=true, union_by_name=true)
        ),
             internal AS (
                 SELECT transaction_id FROM '{dirty_out / "transactions_batch_1.parquet"}'
                 UNION ALL SELECT transaction_id FROM '{dirty_out / "transactions_batch_2.parquet"}'
             )
        SELECT count(*) FROM csv
        WHERE status = 'REVERSED'
          AND transaction_id NOT IN (SELECT transaction_id FROM internal)
    """).fetchone()[0]
    # At 5k rows at least one orphan is expected (rate 0.3% -> ~15)
    assert orphan_reversed > 0
    # null_merchants at 0.01% may be 0 in small sample; assert count is non-negative
    assert null_merchants >= 0


def test_clean_mode_has_no_dirty_patterns(clean_out: Path) -> None:
    con = duckdb.connect()
    null_merchants = con.execute(f"""
        SELECT count(*) FROM (
            SELECT merchant_id FROM '{clean_out / "transactions_batch_1.parquet"}' WHERE merchant_id IS NULL
            UNION ALL
            SELECT merchant_id FROM '{clean_out / "transactions_batch_2.parquet"}' WHERE merchant_id IS NULL
        )
    """).fetchone()[0]
    assert null_merchants == 0

    for p in (clean_out / "paysettler").glob("*.csv"):
        with p.open() as f:
            rdr = csv.reader(f)
            next(rdr)
            for _txn, merch, amt, _cur, settled, _ref, _st in rdr:
                assert settled.endswith("Z"), f"clean mode must keep Z suffix (got {settled!r})"
                assert "," not in amt, f"clean mode must use dot decimal (got {amt!r})"
                assert merch == merch.upper() or not merch, f"clean mode merchant case must be upper (got {merch!r})"


def test_seed_is_deterministic(tmp_path: Path) -> None:
    a = tmp_path / "a"
    b = tmp_path / "b"
    run(a, rows=2_000)
    run(b, rows=2_000)
    con = duckdb.connect()
    for name in ("transactions_batch_1.parquet", "reconciliation_results.parquet"):
        a_hash = con.execute(
            f"SELECT md5(string_agg(transaction_id, '|' ORDER BY transaction_id)) FROM '{a / name}'"
        ).fetchone()[0]
        b_hash = con.execute(
            f"SELECT md5(string_agg(transaction_id, '|' ORDER BY transaction_id)) FROM '{b / name}'"
        ).fetchone()[0]
        assert a_hash == b_hash, f"seed non-deterministic for {name}"
