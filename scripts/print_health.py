#!/usr/bin/env python3
"""CLI wrapper to print a data health report for one or more warehouse tables.

Usage:
    python scripts/print_health.py raw_transactions --pk transaction_id
    python scripts/print_health.py silver_reconciliation_runs --pk id
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Run as a plain script (not `python -m`), so the repo root — not scripts/ —
# must be added to sys.path explicitly for `import src` to resolve.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db import get_connection
from src.observability.health import print_table_health


def main() -> None:
    parser = argparse.ArgumentParser(description="Print a data health report for a warehouse table.")
    parser.add_argument("table", help="Table name to profile")
    parser.add_argument(
        "--pk",
        help="Comma-separated primary key column(s) used for the duplicate-key check",
        default=None,
    )
    args = parser.parse_args()

    pk_cols = [c.strip() for c in args.pk.split(",")] if args.pk else None

    conn = get_connection()
    try:
        print_table_health(conn, args.table, pk_cols=pk_cols)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
