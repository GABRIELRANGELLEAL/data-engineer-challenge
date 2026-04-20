#!/usr/bin/env python3
"""Generate synthetic sample data for the Settlement Reconciliation pipeline.

Produces Debezium-style CDC parquet extracts plus PaySettler CSVs at
arbitrary scale (tested up to 5M internal transactions) and plants
realistic dirty-data issues so an end-to-end pipeline exercises:

    - chunked / memory-aware ingestion
    - schema drift between CDC batches
    - duplicate / null keys
    - malformed numerics (comma decimal)
    - timezone inconsistencies
    - orphan processor records (REVERSED without internal pair)
    - late-arriving settlements (outside 7-day reconciliation window)

Example:
    python scripts/generate_sample_data.py \\
        --rows 1000000 --days 90 --merchants 500 --seed 42 \\
        --out docs/sample-data
"""
from __future__ import annotations

import argparse
import csv
import random
import shutil
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATEGORY_MIX = [
    ("MATCHED", 0.85),
    ("MISMATCHED", 0.05),
    ("UNRECONCILED_PROCESSOR", 0.05),
    ("UNRECONCILED_INTERNAL", 0.05),
]

STATUS_MIX = [("COMPLETED", 0.92), ("PENDING", 0.05), ("FAILED", 0.03)]
CURRENCY_MIX = [("BRL", 0.95), ("USD", 0.04), ("EUR", 0.01)]
PAYMENT_METHODS = ["pix", "credit_card", "debit_card", "boleto"]
CNAE_POOL = ["4712100", "5611203", "4711302", "6201501", "4789099"]

# Fraction of rows affected by each dirty-data pattern when --dirty is on.
DIRTY_RATES = {
    "duplicate_txn": 0.001,      # internal CDC replay: same txn_id twice
    "comma_decimal": 0.0005,     # CSV amount using ',' instead of '.'
    "null_merchant": 0.0001,     # internal row with null merchant_id
    "orphan_reversed": 0.003,    # CSV REVERSED row without any internal pair
    "no_tz_suffix": 0.01,        # CSV settled_at missing trailing 'Z'
    "case_merchant": 0.0005,     # CSV merchant_id lowercased
    "late_settlement": 0.005,    # CSV row settled >7d after created (outside window)
}

CHUNK_SIZE = 100_000

INTERNAL_SCHEMA_BASE = pa.schema([
    ("id", pa.int64()),
    ("transaction_id", pa.string()),
    ("merchant_id", pa.string()),
    ("amount", pa.float64()),
    ("currency", pa.string()),
    ("status", pa.string()),
    ("description", pa.string()),
    ("created_at", pa.string()),
    ("updated_at", pa.string()),
    ("Op", pa.string()),
    ("_timestamp", pa.string()),
])

INTERNAL_SCHEMA_DRIFT = pa.schema(list(INTERNAL_SCHEMA_BASE) + [("payment_method", pa.string())])

RESULT_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("run_id", pa.int64()),
    ("transaction_id", pa.string()),
    ("merchant_id", pa.string()),
    ("category", pa.string()),
    ("internal_amount", pa.float64()),
    ("processor_amount", pa.float64()),
    ("difference", pa.float64()),
    ("created_at", pa.string()),
    ("Op", pa.string()),
    ("_timestamp", pa.string()),
])

RUN_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("reference_date", pa.string()),
    ("file_name", pa.string()),
    ("status", pa.string()),
    ("total_transactions", pa.int64()),
    ("started_at", pa.string()),
    ("completed_at", pa.string()),
    ("created_at", pa.string()),
    ("Op", pa.string()),
    ("_timestamp", pa.string()),
])

MERCHANT_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("merchant_id", pa.string()),
    ("legal_name", pa.string()),
    ("trade_name", pa.string()),
    ("document", pa.string()),
    ("primary_cnae", pa.string()),
    ("created_at", pa.string()),
    ("updated_at", pa.string()),
    ("Op", pa.string()),
    ("_timestamp", pa.string()),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class Config:
    rows: int
    days: int
    merchants: int
    seed: int
    dirty: bool
    out: Path
    start_date: datetime


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def weighted(rng: random.Random, mix):
    r = rng.random()
    acc = 0.0
    for value, weight in mix:
        acc += weight
        if r <= acc:
            return value
    return mix[-1][0]


def make_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def write_parquet(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="snappy")


class StreamingParquetWriter:
    def __init__(self, path: Path, schema: pa.Schema):
        self._path = path
        self._schema = schema
        self._writer: pq.ParquetWriter | None = None
        self._buffer: list[dict] = []

    def append(self, row: dict) -> None:
        self._buffer.append(row)
        if len(self._buffer) >= CHUNK_SIZE:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        table = pa.Table.from_pylist(self._buffer, schema=self._schema)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self._path, self._schema, compression="snappy")
        self._writer.write_table(table)
        self._buffer.clear()

    def close(self) -> None:
        self._flush()
        if self._writer is not None:
            self._writer.close()


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def gen_merchants(cfg: Config, rng: random.Random) -> list[dict]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(1, cfg.merchants + 1):
        created = base + timedelta(days=rng.randint(0, 365))
        updated = created + timedelta(days=rng.randint(0, 180))
        rows.append({
            "id": i,
            "merchant_id": f"MERCH_{i:04d}",
            "legal_name": f"Empresa {i:04d} LTDA",
            "trade_name": f"Loja {i:04d}",
            "document": f"{rng.randint(10**13, 10**14 - 1)}",
            "primary_cnae": rng.choice(CNAE_POOL),
            "created_at": iso(created),
            "updated_at": iso(updated),
            "Op": "I",
            "_timestamp": iso(created),
        })
    return rows


def gen_pipeline_data(cfg: Config, rng: random.Random) -> None:
    """Single-pass generator that writes all pipeline parquet/CSV files."""

    # Chronological split point for schema drift: 70% -> batch_1, 30% -> batch_2
    drift_cutoff = cfg.start_date + timedelta(days=int(cfg.days * 0.7))

    batch1 = StreamingParquetWriter(cfg.out / "transactions_batch_1.parquet", INTERNAL_SCHEMA_BASE)
    batch2 = StreamingParquetWriter(cfg.out / "transactions_batch_2.parquet", INTERNAL_SCHEMA_DRIFT)
    results_writer = StreamingParquetWriter(
        cfg.out / "reconciliation_results.parquet", RESULT_SCHEMA
    )

    paysettler_dir = cfg.out / "paysettler"
    paysettler_dir.mkdir(parents=True, exist_ok=True)
    per_day_rows: dict[str, list[list[str]]] = defaultdict(list)

    merchants = [f"MERCH_{i:04d}" for i in range(1, cfg.merchants + 1)]

    txn_counter = 0
    result_counter = 0
    # reference_date -> run_id (assigned lazily)
    run_ids: dict[str, int] = {}
    next_run_id = 1
    run_row_counts: dict[str, int] = defaultdict(int)

    for _ in range(cfg.rows):
        txn_counter += 1
        day_offset = rng.randint(0, cfg.days - 1)
        day = cfg.start_date + timedelta(days=day_offset)
        created = day + timedelta(seconds=rng.randint(0, 86_399))

        category = weighted(rng, CATEGORY_MIX)
        txn_id = make_uuid(rng)
        merchant = rng.choice(merchants)
        currency = weighted(rng, CURRENCY_MIX)
        status = weighted(rng, STATUS_MIX)
        base_amount = round(min(rng.lognormvariate(4.5, 1.2), 500_000.0), 2)
        base_amount = max(base_amount, 0.50)

        # --- internal (transactions) side ---
        internal_amount: float | None = None
        if category != "UNRECONCILED_PROCESSOR":
            merchant_internal = merchant
            if cfg.dirty and rng.random() < DIRTY_RATES["null_merchant"]:
                merchant_internal = None

            internal_row = {
                "id": txn_counter,
                "transaction_id": txn_id,
                "merchant_id": merchant_internal,
                "amount": base_amount,
                "currency": currency,
                "status": status,
                "description": f"Payment {txn_counter}",
                "created_at": iso(created),
                "updated_at": iso(created + timedelta(minutes=rng.randint(0, 180))),
                "Op": "I",
                "_timestamp": iso(created),
            }
            if created >= drift_cutoff:
                internal_row["payment_method"] = (
                    rng.choice(PAYMENT_METHODS) if rng.random() < 0.6 else None
                )
                batch2.append(internal_row)
            else:
                batch1.append(internal_row)
            internal_amount = base_amount

            # Dirty: duplicate CDC replay (same txn_id appears again as I)
            if cfg.dirty and rng.random() < DIRTY_RATES["duplicate_txn"]:
                dup = dict(internal_row)
                dup["id"] = txn_counter + cfg.rows  # avoid PK clash
                dup["_timestamp"] = iso(created + timedelta(seconds=1))
                if created >= drift_cutoff:
                    batch2.append(dup)
                else:
                    batch1.append(dup)

        # --- processor (PaySettler CSV) side ---
        processor_amount: float | None = None
        reference_date: str | None = None
        if category != "UNRECONCILED_INTERNAL":
            # Late settlement: optionally push settled_at far in the future
            if cfg.dirty and rng.random() < DIRTY_RATES["late_settlement"]:
                settled = created + timedelta(days=rng.randint(8, 15))
            else:
                settled = created + timedelta(hours=rng.randint(1, 48))

            if category == "MISMATCHED":
                delta = round(rng.uniform(0.05, 5.0) * rng.choice([-1, 1]), 2)
                proc_amount = max(0.01, round(base_amount + delta, 2))
            else:
                proc_amount = base_amount
                if rng.random() < 0.3:
                    proc_amount = round(base_amount + rng.choice([-0.01, 0.01]), 2)
            processor_amount = proc_amount

            merchant_csv = merchant
            if cfg.dirty and rng.random() < DIRTY_RATES["case_merchant"]:
                merchant_csv = merchant.lower()

            settled_str = settled.strftime("%Y-%m-%dT%H:%M:%SZ")
            if cfg.dirty and rng.random() < DIRTY_RATES["no_tz_suffix"]:
                settled_str = settled.strftime("%Y-%m-%dT%H:%M:%S")

            amount_str = f"{proc_amount:.2f}"
            if cfg.dirty and rng.random() < DIRTY_RATES["comma_decimal"]:
                amount_str = amount_str.replace(".", ",")

            ps_status = "REVERSED" if rng.random() < 0.02 else "SETTLED"
            reference_date = settled.date().isoformat()

            per_day_rows[reference_date].append([
                txn_id,
                merchant_csv or "",
                amount_str,
                currency,
                settled_str,
                f"PS-{settled.year}-{txn_counter:010d}",
                ps_status,
            ])
            run_row_counts[reference_date] += 1

        # --- reconciliation_results row (one per categorized transaction) ---
        result_counter += 1
        # Anchor category to the processor day when present, else to created day
        anchor_date = reference_date or day.date().isoformat()
        run_id = run_ids.get(anchor_date)
        if run_id is None:
            run_id = next_run_id
            run_ids[anchor_date] = run_id
            next_run_id += 1
            run_row_counts.setdefault(anchor_date, 0)

        difference = None
        if internal_amount is not None and processor_amount is not None:
            difference = round(abs(internal_amount - processor_amount), 2)

        results_writer.append({
            "id": result_counter,
            "run_id": run_id,
            "transaction_id": txn_id,
            "merchant_id": merchant,
            "category": category,
            "internal_amount": internal_amount,
            "processor_amount": processor_amount,
            "difference": difference,
            "created_at": iso(created),
            "Op": "I",
            "_timestamp": iso(created),
        })

    # --- dirty: orphan REVERSED rows in CSV with no internal/result pair ---
    if cfg.dirty:
        n_orphans = max(1, int(cfg.rows * DIRTY_RATES["orphan_reversed"]))
        for _ in range(n_orphans):
            day_offset = rng.randint(0, cfg.days - 1)
            day = cfg.start_date + timedelta(days=day_offset)
            settled = day + timedelta(seconds=rng.randint(0, 86_399))
            ref_date = settled.date().isoformat()
            per_day_rows[ref_date].append([
                make_uuid(rng),
                rng.choice(merchants),
                f"{round(rng.uniform(10, 5000), 2):.2f}",
                "BRL",
                settled.strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"PS-ORPHAN-{rng.randint(1, 10**9):010d}",
                "REVERSED",
            ])
            run_row_counts[ref_date] += 1

    batch1.close()
    batch2.close()
    results_writer.close()

    # --- write per-day PaySettler CSVs + consolidated latest-day file ---
    write_paysettler_csvs(cfg, per_day_rows, paysettler_dir)

    # --- reconciliation_runs (CDC style: I + U rows per run) ---
    write_runs(cfg, run_ids, run_row_counts)


def write_paysettler_csvs(cfg: Config, per_day_rows: dict, paysettler_dir: Path) -> None:
    header = [
        "transaction_id", "merchant_id", "amount", "currency",
        "settled_at", "processor_reference", "status",
    ]
    if not per_day_rows:
        return

    for ref_date, rows in per_day_rows.items():
        path = paysettler_dir / f"settlement_{ref_date}.csv"
        with path.open("w", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            writer.writerows(rows)

    # Backward-compat single file = most recent day
    latest = max(per_day_rows.keys())
    shutil.copyfile(
        paysettler_dir / f"settlement_{latest}.csv",
        cfg.out / "settlement_paysettler.csv",
    )


def write_runs(cfg: Config, run_ids: dict, run_row_counts: dict) -> None:
    rows = []
    for ref_date, run_id in sorted(run_ids.items(), key=lambda kv: kv[1]):
        started = datetime.fromisoformat(f"{ref_date}T02:00:00+00:00")
        completed = started + timedelta(minutes=random.Random(cfg.seed + run_id).randint(5, 90))
        total = run_row_counts.get(ref_date, 0)
        file_name = f"settlement_{ref_date}.csv"
        # I row: IN_PROGRESS
        rows.append({
            "id": run_id,
            "reference_date": ref_date,
            "file_name": file_name,
            "status": "IN_PROGRESS",
            "total_transactions": total,
            "started_at": iso(started),
            "completed_at": None,
            "created_at": iso(started),
            "Op": "I",
            "_timestamp": iso(started),
        })
        # U row: COMPLETED
        rows.append({
            "id": run_id,
            "reference_date": ref_date,
            "file_name": file_name,
            "status": "COMPLETED",
            "total_transactions": total,
            "started_at": iso(started),
            "completed_at": iso(completed),
            "created_at": iso(started),
            "Op": "U",
            "_timestamp": iso(completed),
        })
    write_parquet(cfg.out / "reconciliation_runs.parquet", rows, RUN_SCHEMA)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> Config:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=int, default=1_000_000, help="Total internal transactions (default: 1M)")
    p.add_argument("--days", type=int, default=90, help="Span in days (default: 90)")
    p.add_argument("--merchants", type=int, default=500, help="Number of merchants (default: 500)")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility (default: 42)")
    p.add_argument("--dirty", dest="dirty", action="store_true", default=True,
                   help="Inject dirty-data patterns (default: on)")
    p.add_argument("--clean", dest="dirty", action="store_false",
                   help="Disable dirty-data injection")
    p.add_argument("--out", type=Path, default=Path("docs/sample-data"),
                   help="Output directory (default: docs/sample-data)")
    p.add_argument("--start-date", default="2025-03-01",
                   help="First business date, YYYY-MM-DD (default: 2025-03-01)")
    args = p.parse_args()
    start = datetime.fromisoformat(f"{args.start_date}T00:00:00+00:00")
    return Config(
        rows=args.rows,
        days=args.days,
        merchants=args.merchants,
        seed=args.seed,
        dirty=args.dirty,
        out=args.out,
        start_date=start,
    )


def main() -> None:
    cfg = parse_args()
    cfg.out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(cfg.seed)

    print(f"[generate] rows={cfg.rows:,} days={cfg.days} merchants={cfg.merchants} "
          f"dirty={cfg.dirty} seed={cfg.seed} out={cfg.out}")

    merchants = gen_merchants(cfg, rng)
    write_parquet(cfg.out / "enterprise_company.parquet", merchants, MERCHANT_SCHEMA)
    print(f"[generate]   merchants       {len(merchants):>10,}")

    gen_pipeline_data(cfg, rng)

    # Report counts for sanity
    for name in [
        "transactions_batch_1.parquet",
        "transactions_batch_2.parquet",
        "reconciliation_runs.parquet",
        "reconciliation_results.parquet",
    ]:
        path = cfg.out / name
        if path.exists():
            md = pq.read_metadata(path)
            print(f"[generate]   {name:<35} {md.num_rows:>10,}")

    paysettler_dir = cfg.out / "paysettler"
    if paysettler_dir.exists():
        files = sorted(paysettler_dir.glob("*.csv"))
        total_csv_rows = sum(sum(1 for _ in f.open()) - 1 for f in files)
        print(f"[generate]   paysettler/*.csv ({len(files)} files)         {total_csv_rows:>10,}")

    print("[generate] done")


if __name__ == "__main__":
    main()
