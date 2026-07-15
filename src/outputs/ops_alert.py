"""
Ops alert producer.

Evaluates reconciliation health for a reference_date and writes:
  - {output_dir}/{date}_rates.svg   — bar chart of category rates for the day
  - {output_dir}/{date}_trend.svg   — time-series line chart of rates over last 30 days
  - {output_dir}/{date}_alert.txt   — plain text alert summary

Trigger conditions (evaluated in order):
  1. Latest run for the date has status != COMPLETED.
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

MISMATCHED_THRESHOLD: float = float(os.environ.get("ALERT_MISMATCHED_THRESHOLD", "0.05"))
UNRECONCILED_THRESHOLD: float = float(os.environ.get("ALERT_UNRECONCILED_THRESHOLD", "0.10"))
TREND_SPIKE_MULT: float = float(os.environ.get("ALERT_TREND_SPIKE_MULT", "1.5"))

_CATEGORY_COLORS = {
    "MATCHED": "#4caf50",
    "MISMATCHED": "#ff9800",
    "UNRECONCILED_PROCESSOR": "#f44336",
    "UNRECONCILED_INTERNAL": "#e91e63",
}


def run(
    reference_date: str | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
    output_dir: str | Path = "output/reports",
) -> dict:
    """
    Evaluates reconciliation health and writes two SVG charts and a text alert.

    Args:
        reference_date: Date to evaluate (YYYY-MM-DD). Defaults to latest available.
        output_dir:     Directory where output files are written.

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
        history = _history_rates(_conn, ref_date, days=30)
        alerts = _evaluate(ref_date, run_status, daily, trend)

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        rates_path = _write_rates_chart(ref_date, daily, out_dir)
        trend_path = _write_trend_chart(ref_date, history, out_dir)
        text_path = _write_alert_text(ref_date, run_status, daily, alerts, out_dir)

        level = "CRITICAL" if alerts else "OK"
        logger.info(
            "Ops alert written — level=%s reference_date=%s artifacts=[%s, %s, %s]",
            level, ref_date, rates_path.name, trend_path.name, text_path.name,
        )
        return {
            "reference_date": ref_date,
            "run_status": run_status,
            "alert_level": level,
            "alerts": alerts,
            "rates_path": str(rates_path),
            "trend_path": str(trend_path),
            "alert_text_path": str(text_path),
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


def _history_rates(
    conn: duckdb.DuckDBPyConnection, ref_date: str, days: int = 30
) -> list[dict]:
    rows = conn.execute(f"""
        SELECT reference_date, category, pct_of_total
        FROM gold_ops_reconciliation_daily
        WHERE reference_date > (CAST('{ref_date}' AS DATE) - INTERVAL '{days} days')
          AND reference_date <= CAST('{ref_date}' AS DATE)
        ORDER BY reference_date, category
    """).fetchall()
    return [
        {
            "reference_date": str(r[0]),
            "category": r[1],
            "pct_of_total": float(r[2]) if r[2] is not None else 0.0,
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


# ─── Chart: daily rates bar chart ────────────────────────────────────────────

def _write_rates_chart(ref_date: str, daily: list[dict], out_dir: Path) -> Path:
    width = 500
    margin = {"top": 44, "right": 24, "bottom": 72, "left": 56}
    chart_w = width - margin["left"] - margin["right"]
    chart_h = 160
    height = chart_h + margin["top"] + margin["bottom"]

    bars_data = [(r["category"], r["pct_of_total"]) for r in daily]
    max_val = max((v for _, v in bars_data), default=1.0) or 1.0
    n = max(len(bars_data), 1)
    slot = chart_w // n
    bar_w = max(slot * 3 // 4, 4)

    elements: list[str] = []
    for i, (label, val) in enumerate(bars_data):
        bh = max(int(val / max_val * chart_h), 1)
        x = margin["left"] + i * slot + (slot - bar_w) // 2
        y = margin["top"] + chart_h - bh
        fill = _CATEGORY_COLORS.get(label, "#9e9e9e")
        short = label.replace("UNRECONCILED_", "UNR_")
        cx = x + bar_w // 2
        elements.append(
            f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bh}" fill="{fill}" rx="3"/>'
            f'<text x="{cx}" y="{y - 5}" text-anchor="middle" font-size="11" fill="#333">'
            f"{val:.1%}</text>"
            f'<text x="{cx}" y="{margin["top"] + chart_h + 18}" '
            f'text-anchor="middle" font-size="9" fill="#555">{short}</text>'
        )

    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">'
        f'<rect width="{width}" height="{height}" fill="#fafafa" rx="6"/>'
        f'<text x="{width // 2}" y="26" text-anchor="middle" font-size="13" '
        f'font-weight="bold" fill="#222">Reconciliation Rates — {ref_date}</text>'
        + "".join(elements)
        + "</svg>"
    )
    path = out_dir / f"{ref_date}_rates.svg"
    path.write_text(svg, encoding="utf-8")
    return path


# ─── Chart: time-series trend line chart ─────────────────────────────────────

def _write_trend_chart(ref_date: str, history: list[dict], out_dir: Path) -> Path:
    width, height = 600, 280
    margin = {"top": 44, "right": 120, "bottom": 48, "left": 52}
    chart_w = width - margin["left"] - margin["right"]
    chart_h = height - margin["top"] - margin["bottom"]

    # Group by category → list of (date, pct) sorted by date
    series: dict[str, list[tuple[str, float]]] = {}
    for row in history:
        series.setdefault(row["category"], []).append(
            (row["reference_date"], row["pct_of_total"])
        )
    for points in series.values():
        points.sort(key=lambda x: x[0])

    all_dates = sorted({r["reference_date"] for r in history})
    if not all_dates:
        svg = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">'
            f'<text x="{width//2}" y="{height//2}" text-anchor="middle" font-size="13" fill="#999">'
            f'No trend data available</text></svg>'
        )
        path = out_dir / f"{ref_date}_trend.svg"
        path.write_text(svg, encoding="utf-8")
        return path

    max_pct = max(r["pct_of_total"] for r in history) or 1.0
    n_dates = max(len(all_dates) - 1, 1)
    date_index = {d: i for i, d in enumerate(all_dates)}

    def _x(date: str) -> float:
        return margin["left"] + date_index[date] / n_dates * chart_w

    def _y(pct: float) -> float:
        return margin["top"] + chart_h - (pct / max_pct * chart_h)

    elements: list[str] = []

    # Y-axis gridlines and labels
    for tick in [0.0, 0.25, 0.5, 0.75, 1.0]:
        yy = _y(max_pct * tick)
        elements.append(
            f'<line x1="{margin["left"]}" y1="{yy}" x2="{margin["left"] + chart_w}" y2="{yy}" '
            f'stroke="#e0e0e0" stroke-width="1"/>'
            f'<text x="{margin["left"] - 4}" y="{yy + 4}" text-anchor="end" '
            f'font-size="9" fill="#888">{max_pct * tick:.0%}</text>'
        )

    # X-axis date labels (first, middle, last)
    label_indices = {0, len(all_dates) // 2, len(all_dates) - 1}
    for idx in label_indices:
        if idx < len(all_dates):
            d = all_dates[idx]
            xx = _x(d)
            elements.append(
                f'<text x="{xx}" y="{margin["top"] + chart_h + 16}" '
                f'text-anchor="middle" font-size="9" fill="#888">{d}</text>'
            )

    # Lines per category
    for cat, points in series.items():
        color = _CATEGORY_COLORS.get(cat, "#9e9e9e")
        if len(points) < 2:
            if points:
                px, py = _x(points[0][0]), _y(points[0][1])
                elements.append(
                    f'<circle cx="{px}" cy="{py}" r="3" fill="{color}"/>'
                )
            continue
        coords = " ".join(f"{_x(d)},{_y(v)}" for d, v in points)
        elements.append(
            f'<polyline points="{coords}" fill="none" stroke="{color}" '
            f'stroke-width="2" stroke-linejoin="round"/>'
        )
        # Dot on last point
        lx, ly = _x(points[-1][0]), _y(points[-1][1])
        elements.append(f'<circle cx="{lx}" cy="{ly}" r="3" fill="{color}"/>')

    # Legend on the right
    categories = list(series.keys())
    for i, cat in enumerate(sorted(categories)):
        color = _CATEGORY_COLORS.get(cat, "#9e9e9e")
        short = cat.replace("UNRECONCILED_", "UNR_")
        ly = margin["top"] + i * 18
        elements.append(
            f'<rect x="{margin["left"] + chart_w + 8}" y="{ly}" width="10" height="10" fill="{color}" rx="2"/>'
            f'<text x="{margin["left"] + chart_w + 22}" y="{ly + 9}" font-size="9" fill="#444">{short}</text>'
        )

    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">'
        f'<rect width="{width}" height="{height}" fill="#fafafa" rx="6"/>'
        f'<text x="{(margin["left"] + margin["left"] + chart_w) // 2}" y="26" '
        f'text-anchor="middle" font-size="13" font-weight="bold" fill="#222">'
        f'Reconciliation Trend (last 30 days)</text>'
        + "".join(elements)
        + "</svg>"
    )
    path = out_dir / f"{ref_date}_trend.svg"
    path.write_text(svg, encoding="utf-8")
    return path


# ─── Text alert ──────────────────────────────────────────────────────────────

def _write_alert_text(
    ref_date: str,
    run_status: str,
    daily: list[dict],
    alerts: list[dict],
    out_dir: Path,
) -> Path:
    level = "CRITICAL" if alerts else "OK"
    total = sum(r["txn_count"] for r in daily)
    matched_pct = next(
        (r["pct_of_total"] for r in daily if r["category"] == "MATCHED"), 0.0
    )

    lines: list[str] = [
        f"Reconciliation Alert — {ref_date}",
        "=" * 40,
        f"Status      : {level}",
        f"Run status  : {run_status}",
        f"Total txns  : {total:,}",
        f"Match rate  : {matched_pct:.1%}",
        "",
        "Category breakdown:",
    ]
    for r in daily:
        lines.append(f"  {r['category']:<30} {r['pct_of_total']:.1%}  ({r['txn_count']:,} txns)")

    if alerts:
        lines += ["", "Active alerts:"]
        for a in alerts:
            lines.append(f"  [!] {a['message']}")
    else:
        lines += ["", "No alerts — all rates within thresholds."]

    path = out_dir / f"{ref_date}_alert.txt"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    ref = sys.argv[1] if len(sys.argv) > 1 else None
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "output/reports"
    result = run(reference_date=ref, output_dir=out_dir)
    print(json.dumps(result, indent=2, default=str))
