"""
Ops alert producer.

Evaluates reconciliation health for a reference_date and emits simulated Slack artifacts:
  - output/alerts/{date}_chart.svg   — bar chart of category rates
  - output/alerts/{date}_alert.json  — Slack Block Kit payload

Trigger conditions (evaluated in order):
  1. Latest run for the date has status != COMPLETED (checked in silver — a failed run
     may have no gold rows at all, so we must check silver_reconciliation_runs directly).
  2. A category rate exceeds its configured threshold AND is above TREND_SPIKE_MULT × 7-day avg
     (or has no 7-day history yet, in which case the threshold alone triggers).

Alert thresholds live here, not in SQL — they are calibration knobs, not data logic.
"""
import json
import logging
import os
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_OUTPUT_DIR = Path("output/alerts")

MISMATCHED_THRESHOLD: float = float(os.environ.get("ALERT_MISMATCHED_THRESHOLD", "0.05"))
UNRECONCILED_THRESHOLD: float = float(os.environ.get("ALERT_UNRECONCILED_THRESHOLD", "0.10"))
TREND_SPIKE_MULT: float = float(os.environ.get("ALERT_TREND_SPIKE_MULT", "1.5"))

_CATEGORY_EMOJI = {
    "MATCHED": ":white_check_mark:",
    "MISMATCHED": ":warning:",
    "UNRECONCILED_PROCESSOR": ":x:",
    "UNRECONCILED_INTERNAL": ":x:",
}


def run(
    reference_date: str | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """
    Evaluates reconciliation health and writes simulated Slack artifacts.

    Args:
        reference_date: Date to evaluate (YYYY-MM-DD). Defaults to latest available.

    Returns:
        Dict with reference_date, run_status, alert_level, alerts list, and output paths.
    """
    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        ref_date = reference_date or _latest_reference_date(_conn)
        if ref_date is None:
            logger.warning("No reconciliation data found — nothing to alert on.")
            return {"status": "no_data"}

        run_status = _run_status(_conn, ref_date)
        daily = _daily_rates(_conn, ref_date)
        trend = _trend_rates(_conn, ref_date)
        alerts = _evaluate(ref_date, run_status, daily, trend)

        _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        chart_path = _write_chart(ref_date, daily)
        payload = _block_kit(ref_date, run_status, daily, alerts)
        payload_path = _OUTPUT_DIR / f"{ref_date}_alert.json"
        payload_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        level = "CRITICAL" if alerts else "OK"
        logger.info(
            "[SIMULATED SLACK] channel=#ops-alerts level=%s reference_date=%s "
            "artifacts=[%s, %s]",
            level,
            ref_date,
            chart_path.name,
            payload_path.name,
        )
        return {
            "reference_date": ref_date,
            "run_status": run_status,
            "alert_level": level,
            "alerts": alerts,
            "chart_path": str(chart_path),
            "payload_path": str(payload_path),
        }
    finally:
        if owns_conn:
            _conn.close()


# ─── Data accessors ──────────────────────────────────────────────────────────

def _latest_reference_date(conn: duckdb.DuckDBPyConnection) -> str | None:
    row = conn.execute(
        "SELECT MAX(reference_date) FROM gold_ops_reconciliation_daily"
    ).fetchone()
    val = row[0] if row else None
    return str(val) if val else None


def _run_status(conn: duckdb.DuckDBPyConnection, ref_date: str) -> str:
    row = conn.execute(f"""
        SELECT status
        FROM silver_reconciliation_runs
        WHERE reference_date = CAST('{ref_date}' AS DATE)
        ORDER BY started_at DESC, id DESC
        LIMIT 1
    """).fetchone()
    return row[0] if row else "NO_RUN"


def _daily_rates(conn: duckdb.DuckDBPyConnection, ref_date: str) -> list[dict]:
    rows = conn.execute(f"""
        SELECT category, txn_count, pct_of_total, internal_amount_sum, processor_amount_sum
        FROM gold_ops_reconciliation_daily
        WHERE reference_date = CAST('{ref_date}' AS DATE)
        ORDER BY category
    """).fetchall()
    return [
        {
            "category": r[0],
            "txn_count": r[1],
            "pct_of_total": float(r[2]) if r[2] is not None else 0.0,
            "internal_amount_sum": float(r[3]) if r[3] is not None else 0.0,
            "processor_amount_sum": float(r[4]) if r[4] is not None else 0.0,
        }
        for r in rows
    ]


def _trend_rates(conn: duckdb.DuckDBPyConnection, ref_date: str) -> list[dict]:
    rows = conn.execute(f"""
        SELECT category, pct_of_total, pct_of_total_7d_avg
        FROM gold_ops_reconciliation_trend
        WHERE reference_date = CAST('{ref_date}' AS DATE)
        ORDER BY category
    """).fetchall()
    return [
        {
            "category": r[0],
            "pct_of_total": float(r[1]) if r[1] is not None else 0.0,
            "pct_of_total_7d_avg": float(r[2]) if r[2] is not None else None,
        }
        for r in rows
    ]


# ─── Alert logic ─────────────────────────────────────────────────────────────

def _evaluate(
    ref_date: str,
    run_status: str,
    daily: list[dict],
    trend: list[dict],
) -> list[dict]:
    alerts: list[dict] = []

    if run_status != "COMPLETED":
        alerts.append({
            "type": "RUN_FAILED",
            "message": (
                f"Run for {ref_date} ended with status {run_status!r} — "
                "reconciliation may be incomplete or absent."
            ),
        })

    trend_by_cat = {r["category"]: r for r in trend}
    thresholds = {
        "MISMATCHED": MISMATCHED_THRESHOLD,
        "UNRECONCILED_PROCESSOR": UNRECONCILED_THRESHOLD,
        "UNRECONCILED_INTERNAL": UNRECONCILED_THRESHOLD,
    }

    for row in daily:
        cat = row["category"]
        threshold = thresholds.get(cat)
        if threshold is None:
            continue
        pct = row["pct_of_total"]
        if pct <= threshold:
            continue
        avg = (trend_by_cat.get(cat) or {}).get("pct_of_total_7d_avg")
        if avg is not None and pct <= avg * TREND_SPIKE_MULT:
            continue
        alerts.append({
            "type": "RATE_SPIKE",
            "category": cat,
            "rate": pct,
            "threshold": threshold,
            "7d_avg": avg,
            "message": (
                f"{cat} rate {pct:.1%} exceeds threshold {threshold:.1%}"
                + (f" and is {pct / avg:.1f}× the 7-day avg ({avg:.1%})" if avg else "")
            ),
        })

    return alerts


# ─── Artifact renderers ───────────────────────────────────────────────────────

def _write_chart(ref_date: str, daily: list[dict]) -> Path:
    svg = _svg_bar_chart(
        title=f"Reconciliation Rates — {ref_date}",
        bars=[(r["category"], r["pct_of_total"]) for r in daily],
    )
    path = _OUTPUT_DIR / f"{ref_date}_chart.svg"
    path.write_text(svg, encoding="utf-8")
    return path


def _svg_bar_chart(title: str, bars: list[tuple[str, float]]) -> str:
    width = 500
    margin = {"top": 44, "right": 24, "bottom": 72, "left": 56}
    chart_w = width - margin["left"] - margin["right"]
    chart_h = 160
    height = chart_h + margin["top"] + margin["bottom"]

    max_val = max((v for _, v in bars), default=1.0) or 1.0
    n = max(len(bars), 1)
    slot = chart_w // n
    bar_w = max(slot * 3 // 4, 4)

    _colors = {
        "MATCHED": "#4caf50",
        "MISMATCHED": "#ff9800",
        "UNRECONCILED_PROCESSOR": "#f44336",
        "UNRECONCILED_INTERNAL": "#e91e63",
    }

    elements: list[str] = []
    for i, (label, val) in enumerate(bars):
        bh = max(int(val / max_val * chart_h), 1)
        x = margin["left"] + i * slot + (slot - bar_w) // 2
        y = margin["top"] + chart_h - bh
        fill = _colors.get(label, "#9e9e9e")
        short = label.replace("UNRECONCILED_", "UNR_")
        cx = x + bar_w // 2
        elements.append(
            f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bh}" fill="{fill}" rx="3"/>'
            f'<text x="{cx}" y="{y - 5}" text-anchor="middle" font-size="11" fill="#333">'
            f"{val:.1%}</text>"
            f'<text x="{cx}" y="{margin["top"] + chart_h + 18}" '
            f'text-anchor="middle" font-size="9" fill="#555">{short}</text>'
        )

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">'
        f'<rect width="{width}" height="{height}" fill="#fafafa" rx="6"/>'
        f'<text x="{width // 2}" y="26" text-anchor="middle" font-size="13" '
        f'font-weight="bold" fill="#222">{title}</text>'
        + "".join(elements)
        + "</svg>"
    )


def _block_kit(
    ref_date: str,
    run_status: str,
    daily: list[dict],
    alerts: list[dict],
) -> dict:
    level_icon = ":red_circle:" if alerts else ":large_green_circle:"
    level_text = "CRITICAL" if alerts else "OK"
    total = sum(r["txn_count"] for r in daily)
    matched_pct = next(
        (r["pct_of_total"] for r in daily if r["category"] == "MATCHED"), 0.0
    )

    rate_lines = "\n".join(
        f"  {_CATEGORY_EMOJI.get(r['category'], '•')} *{r['category']}*: "
        f"{r['pct_of_total']:.1%} ({r['txn_count']:,} txns)"
        for r in daily
    )

    alert_blocks: list[dict] = []
    if alerts:
        alert_text = "\n".join(f"  :warning: {a['message']}" for a in alerts)
        alert_blocks = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Active Alerts*\n{alert_text}"},
            },
            {"type": "divider"},
        ]

    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{level_icon} Reconciliation {level_text} — {ref_date}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Status*\n{level_text}"},
                    {"type": "mrkdwn", "text": f"*Run Status*\n{run_status}"},
                    {"type": "mrkdwn", "text": f"*Total Transactions*\n{total:,}"},
                    {"type": "mrkdwn", "text": f"*Match Rate*\n{matched_pct:.1%}"},
                ],
            },
            {"type": "divider"},
            *alert_blocks,
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Category Breakdown*\n{rate_lines}",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"_Reference date: {ref_date} · Simulated Slack delivery_",
                    }
                ],
            },
        ]
    }


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    ref = sys.argv[1] if len(sys.argv) > 1 else None
    result = run(reference_date=ref)
    print(json.dumps(result, indent=2, default=str))
