"""
CFO reconciliation report producer.

Aggregates the entire available history from gold_cfo_weekly_summary,
gold_cfo_weekly_merchant_ranking, and gold_ops_reconciliation_daily, and
renders an HTML report with:
  - KPI headline (total BRL volume, total transactions)
  - Volume by category — total (SVG bar chart + table)
  - Volume by category — per day (stacked SVG bar chart + table), queried from
    gold_ops_reconciliation_daily since the weekly gold tables don't carry
    daily grain
  - Top-N merchant risk ranking summed across the full period (default N=10,
    overridable via CFO_REPORT_TOP_N)

Writes to:  {output_dir}/{start}_{end}_cfo_report.html
"""
import html
import json
import logging
import os
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_TOP_N: int = int(os.environ.get("CFO_REPORT_TOP_N", "10"))

_CATEGORY_COLORS = {
    "MATCHED": "#4caf50",
    "MISMATCHED": "#ff9800",
    "UNRECONCILED_PROCESSOR": "#f44336",
    "UNRECONCILED_INTERNAL": "#e91e63",
}
_RISK_CATEGORIES = ("MATCHED", "MISMATCHED", "UNRECONCILED_PROCESSOR", "UNRECONCILED_INTERNAL")


def run(
    conn: duckdb.DuckDBPyConnection | None = None,
    output_dir: str | Path = "outputs",
) -> dict:
    """
    Renders the full-period CFO report as an HTML file.

    Args:
        output_dir:  Directory where the report file is written.

    Returns:
        Dict with period_start, period_end, and report_path.
    """
    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        period_range = _get_period_range(_conn)
        if period_range is None:
            logger.warning("No CFO data found — nothing to report.")
            return {"status": "no_data"}
        start, end = period_range

        summary = _get_full_summary(_conn)
        daily = _get_daily_by_category(_conn)
        ranking = _get_full_ranking(_conn, _TOP_N)

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        chart_svg = _summary_chart(summary)
        daily_chart_svg = _daily_chart(daily)
        content = _render_html(start, end, summary, daily, ranking, chart_svg, daily_chart_svg)

        report_path = out_dir / f"{start.replace('-', '')}_{end.replace('-', '')}_cfo_report.html"
        report_path.write_text(content, encoding="utf-8")

        logger.info("CFO report written to %s", report_path)
        return {
            "period_start": start,
            "period_end": end,
            "report_path": str(report_path),
        }
    finally:
        if owns_conn:
            _conn.close()


# ─── Data accessors ──────────────────────────────────────────────────────────

def _get_period_range(conn: duckdb.DuckDBPyConnection) -> tuple[str, str] | None:
    row = conn.execute(
        "SELECT MIN(week_start), MAX(week_end) FROM gold_cfo_weekly_summary"
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0]), str(row[1])


def _get_full_summary(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Category totals across every available week."""
    rows = conn.execute("""
        SELECT category, SUM(txn_count) AS txn_count, SUM(amount_brl) AS amount_brl
        FROM gold_cfo_weekly_summary
        GROUP BY category
        ORDER BY category
    """).fetchall()
    return [
        {
            "category": r[0],
            "txn_count": r[1],
            "amount_brl": float(r[2]) if r[2] is not None else 0.0,
        }
        for r in rows
    ]


def _get_daily_by_category(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Daily category volumes across the entire period, from gold_ops_reconciliation_daily.
    That view already applies the winning-run-per-reference_date policy in silver.
    amount_brl = COALESCE(processor_amount_sum, internal_amount_sum): within a single
    category, processor_amount is either always present or always NULL (that's what
    defines the category), so this is equivalent to CFO's row-wise
    COALESCE(processor_amount, internal_amount) convention, just pre-aggregated.
    """
    rows = conn.execute("""
        SELECT
            reference_date,
            category,
            txn_count,
            COALESCE(processor_amount_sum, internal_amount_sum) AS amount_brl
        FROM gold_ops_reconciliation_daily
        WHERE category IS NOT NULL
        ORDER BY reference_date, category
    """).fetchall()
    return [
        {
            "reference_date": str(r[0]),
            "category": r[1],
            "txn_count": r[2],
            "amount_brl": float(r[3]) if r[3] is not None else 0.0,
        }
        for r in rows
    ]


def _get_full_ranking(conn: duckdb.DuckDBPyConnection, top_n: int) -> list[dict]:
    """Top merchants by non-matched risk amount, summed across every available week."""
    rows = conn.execute(f"""
        SELECT
            merchant_id,
            MAX(legal_name)  AS legal_name,
            MAX(trade_name)  AS trade_name,
            SUM(txn_count)   AS txn_count,
            SUM(amount_brl)  AS amount_brl
        FROM gold_cfo_weekly_merchant_ranking
        GROUP BY merchant_id
        ORDER BY amount_brl DESC
        LIMIT {top_n}
    """).fetchall()
    return [
        {
            "merchant_id": r[0],
            "legal_name": r[1] or "",
            "trade_name": r[2] or "",
            "txn_count": r[3],
            "amount_brl": float(r[4]) if r[4] is not None else 0.0,
        }
        for r in rows
    ]


# ─── Chart renderers ──────────────────────────────────────────────────────────

def _summary_chart(summary: list[dict]) -> str:
    """Horizontal SVG bar chart of BRL amount by category."""
    bar_h, gap = 28, 8
    label_w, value_w = 160, 140
    chart_max_w = 300
    total_brl = sum(r["amount_brl"] for r in summary) or 1.0
    svg_h = len(summary) * (bar_h + gap) + gap + 10
    svg_w = label_w + chart_max_w + value_w

    bars: list[str] = []
    for i, row in enumerate(summary):
        pct = row["amount_brl"] / total_brl
        bw = int(pct * chart_max_w)
        y = gap + i * (bar_h + gap)
        fill = _CATEGORY_COLORS.get(row["category"], "#9e9e9e")
        short = row["category"].replace("UNRECONCILED_", "UNR_")
        bars.append(
            f'<text x="{label_w - 6}" y="{y + 18}" text-anchor="end" '
            f'font-size="10" fill="#444">{short}</text>'
            f'<rect x="{label_w}" y="{y}" width="{bw}" height="{bar_h}" fill="{fill}" rx="3"/>'
            f'<text x="{label_w + bw + 6}" y="{y + 18}" font-size="10" fill="#555">'
            f'R$ {row["amount_brl"]:,.2f} ({pct:.1%})</text>'
        )

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{svg_w}" height="{svg_h}">'
        + "".join(bars)
        + "</svg>"
    )


_MATCHED_CATEGORIES = ("MATCHED",)
_NON_MATCHED_CATEGORIES = tuple(c for c in _RISK_CATEGORIES if c != "MATCHED")


def _line_chart_svg(
    by_date: dict[str, dict[str, float]], dates: list[str], categories: tuple[str, ...]
) -> str:
    """Multi-series SVG line chart of BRL volume for the given categories, one point per day."""
    margin = {"top": 20, "right": 16, "bottom": 60, "left": 70}
    chart_h = 160
    slot = 26
    chart_w = max(len(dates) - 1, 1) * slot
    width = margin["left"] + chart_w + margin["right"]
    show_legend = len(categories) > 1
    height = margin["top"] + chart_h + margin["bottom"] + (16 if show_legend else 0)

    max_val = max(
        (by_date[d].get(cat, 0.0) for d in dates for cat in categories), default=0.0
    ) or 1.0

    def y_of(amt: float) -> float:
        return margin["top"] + chart_h - (amt / max_val * chart_h)

    elements: list[str] = []

    # Y-axis gridlines + value labels (0%, 25%, 50%, 75%, 100% of max).
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        gy = y_of(max_val * frac)
        elements.append(
            f'<line x1="{margin["left"]}" y1="{gy:.1f}" x2="{margin["left"] + chart_w}" y2="{gy:.1f}" '
            f'stroke="#eee" stroke-width="1"/>'
            f'<text x="{margin["left"] - 6}" y="{gy + 3:.1f}" text-anchor="end" font-size="9" fill="#888">'
            f'R$ {max_val * frac:,.0f}</text>'
        )

    # One polyline + markers per category.
    for cat in categories:
        color = _CATEGORY_COLORS.get(cat, "#9e9e9e")
        points = [
            (margin["left"] + i * slot, y_of(by_date[d].get(cat, 0.0)))
            for i, d in enumerate(dates)
        ]
        path = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        elements.append(f'<polyline points="{path}" fill="none" stroke="{color}" stroke-width="2"/>')
        for (x, y), d in zip(points, dates):
            amt = by_date[d].get(cat, 0.0)
            elements.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="{color}">'
                f'<title>{d} — {cat}: R$ {amt:,.2f}</title></circle>'
            )

    # X-axis day labels.
    for i, d in enumerate(dates):
        label_x = margin["left"] + i * slot
        label_y = margin["top"] + chart_h + 14
        elements.append(
            f'<text x="{label_x:.1f}" y="{label_y}" text-anchor="end" font-size="9" fill="#555" '
            f'transform="rotate(-60 {label_x:.1f} {label_y})">{d[5:]}</text>'
        )

    legend = ""
    if show_legend:
        legend = "".join(
            f'<rect x="{margin["left"] + i * 150}" y="{height - 14}" width="10" height="10" '
            f'fill="{_CATEGORY_COLORS[c]}"/>'
            f'<text x="{margin["left"] + i * 150 + 14}" y="{height - 5}" font-size="9" fill="#444">'
            f'{c.replace("UNRECONCILED_", "UNR_")}</text>'
            for i, c in enumerate(categories)
        )

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">'
        + "".join(elements)
        + legend
        + "</svg>"
    )


def _daily_chart(daily: list[dict]) -> str:
    """Two stacked line charts: Matched volume on top, non-matched categories below."""
    by_date: dict[str, dict[str, float]] = {}
    for r in daily:
        by_date.setdefault(r["reference_date"], {})[r["category"]] = r["amount_brl"]
    dates = sorted(by_date)
    if not dates:
        return "<p style='color:#999;font-size:12px'>No daily data available.</p>"

    matched_svg = _line_chart_svg(by_date, dates, _MATCHED_CATEGORIES)
    non_matched_svg = _line_chart_svg(by_date, dates, _NON_MATCHED_CATEGORIES)

    return (
        '<div style="font-size:11px;font-weight:bold;color:#444;margin-bottom:2px">Matched</div>'
        f'{matched_svg}'
        '<div style="font-size:11px;font-weight:bold;color:#444;margin:10px 0 2px">Non-Matched '
        '(Mismatched, Unreconciled Processor, Unreconciled Internal)</div>'
        f'{non_matched_svg}'
    )


def _daily_table_rows(daily: list[dict]) -> str:
    by_date: dict[str, dict[str, float]] = {}
    txn_by_date: dict[str, int] = {}
    for r in daily:
        by_date.setdefault(r["reference_date"], {})[r["category"]] = r["amount_brl"]
        txn_by_date[r["reference_date"]] = txn_by_date.get(r["reference_date"], 0) + r["txn_count"]

    rows: list[str] = []
    for d in sorted(by_date):
        cells = "".join(
            f"<td style='text-align:right'>R$ {by_date[d].get(c, 0.0):,.2f}</td>"
            for c in _RISK_CATEGORIES
        )
        total = sum(by_date[d].values())
        rows.append(
            f"<tr><td>{d}</td>{cells}"
            f"<td style='text-align:right'><strong>R$ {total:,.2f}</strong></td>"
            f"<td style='text-align:right'>{txn_by_date[d]:,}</td></tr>"
        )
    return "".join(rows)


# ─── HTML renderer ────────────────────────────────────────────────────────────

def _render_html(
    period_start: str,
    period_end: str,
    summary: list[dict],
    daily: list[dict],
    ranking: list[dict],
    chart_svg: str,
    daily_chart_svg: str,
) -> str:
    total_brl = sum(r["amount_brl"] for r in summary)
    total_txn = sum(r["txn_count"] for r in summary)

    summary_rows = "".join(
        "<tr>"
        f"<td>{html.escape(r['category'])}</td>"
        f"<td style='text-align:right'>{r['txn_count']:,}</td>"
        f"<td style='text-align:right'>R$ {r['amount_brl']:,.2f}</td>"
        "</tr>"
        for r in summary
    )

    daily_table_rows = _daily_table_rows(daily)

    ranking_rows = "".join(
        "<tr>"
        f"<td>{i}</td>"
        f"<td>{html.escape(r['trade_name'] or r['legal_name'] or r['merchant_id'] or '(unknown merchant)')}</td>"
        f"<td style='text-align:right'>{r['txn_count']:,}</td>"
        f"<td style='text-align:right'>R$ {r['amount_brl']:,.2f}</td>"
        "</tr>"
        for i, r in enumerate(ranking, 1)
    )

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <title>CFO Reconciliation Report — {period_start} to {period_end}</title>
  <style>
    body {{font-family:Arial,sans-serif;max-width:960px;margin:0 auto;padding:24px;color:#222}}
    h1  {{font-size:20px;border-bottom:2px solid #1a73e8;padding-bottom:8px;margin-bottom:4px}}
    h2  {{font-size:14px;color:#444;margin-top:28px;text-transform:uppercase;letter-spacing:.05em}}
    table {{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
    th {{background:#f0f4ff;text-align:left;padding:7px 10px;border-bottom:2px solid #c5d0e6}}
    td {{padding:6px 10px;border-bottom:1px solid #edf0f8}}
    .kpi-row {{display:flex;gap:12px;margin:16px 0}}
    .kpi {{flex:1;background:#f7f9ff;border:1px solid #d0daf5;border-radius:8px;
            padding:14px 18px;text-align:center}}
    .kpi-val {{font-size:24px;font-weight:bold;color:#1a73e8}}
    .kpi-lbl {{font-size:11px;color:#666;margin-top:4px}}
    .chart-scroll {{overflow-x:auto;border:1px solid #eee;border-radius:6px;padding:8px 0}}
    .footer {{font-size:11px;color:#999;margin-top:36px;border-top:1px solid #eee;padding-top:10px}}
  </style>
</head>
<body>
  <h1>CFO Reconciliation Report</h1>
  <p style="color:#666;font-size:13px">
    <strong>{period_start}</strong> to <strong>{period_end}</strong>
    &bull; Generated automatically
  </p>

  <div class="kpi-row">
    <div class="kpi">
      <div class="kpi-val">R$ {total_brl:,.0f}</div>
      <div class="kpi-lbl">Total BRL Volume</div>
    </div>
    <div class="kpi">
      <div class="kpi-val">{total_txn:,}</div>
      <div class="kpi-lbl">Total Transactions</div>
    </div>
  </div>

  <h2>Volume by Category — Total</h2>
  {chart_svg}

  <table>
    <thead>
      <tr><th>Category</th><th>Transactions</th><th>Amount (BRL)</th></tr>
    </thead>
    <tbody>{summary_rows}</tbody>
  </table>

  <h2>Volume by Category — Per Day</h2>
  <div class="chart-scroll">{daily_chart_svg}</div>
  <table>
    <thead>
      <tr>
        <th>Date</th><th>Matched</th><th>Mismatched</th><th>Unrec. Processor</th>
        <th>Unrec. Internal</th><th>Total</th><th>Txns</th>
      </tr>
    </thead>
    <tbody>{daily_table_rows}</tbody>
  </table>

  <h2>Top {len(ranking)} Merchants by Risk Amount</h2>
  <p style="font-size:12px;color:#666;margin:4px 0 8px">
    Non-matched transactions only (MISMATCHED, UNRECONCILED_PROCESSOR, UNRECONCILED_INTERNAL),
    summed across the full period.
  </p>
  <table>
    <thead>
      <tr><th>#</th><th>Merchant</th><th>Transactions</th><th>Amount (BRL)</th></tr>
    </thead>
    <tbody>{ranking_rows}</tbody>
  </table>

  <div class="footer">
    Data source: <code>gold_cfo_weekly_summary</code>, <code>gold_cfo_weekly_merchant_ranking</code>,
    <code>gold_ops_reconciliation_daily</code> &bull; Generated automatically
  </div>
</body>
</html>"""


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "outputs"
    result = run(output_dir=out_dir)
    print(json.dumps(result, indent=2, default=str))
