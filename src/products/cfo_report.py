"""
CFO weekly reconciliation report producer.

Reads from gold_cfo_weekly_summary and gold_cfo_weekly_merchant_ranking and renders
an HTML report with:
  - KPI headline (total BRL volume, transaction count, week-over-week change)
  - SVG bar chart of BRL volume by category
  - Summary table by category with week-over-week delta
  - Top-N merchant risk ranking (default N=10, overridable via CFO_REPORT_TOP_N)

Writes to:  {output_dir}/{YYYYMMDD}_cfo_report.html
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


def run(
    week_start: str | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
    output_dir: str | Path = "output/reports",
) -> dict:
    """
    Renders the weekly CFO report as an HTML file.

    Args:
        week_start:  ISO week start (Monday, YYYY-MM-DD). Defaults to latest available week.
        output_dir:  Directory where the report file is written.

    Returns:
        Dict with week_start and report_path.
    """
    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        ws = week_start or _latest_week_start(_conn)
        if ws is None:
            logger.warning("No CFO weekly data found — nothing to report.")
            return {"status": "no_data"}

        summary = _get_summary(_conn, ws)
        prev_summary = _get_prev_summary(_conn, ws)
        ranking = _get_ranking(_conn, ws, _TOP_N)

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        chart_svg = _summary_chart(summary)
        content = _render_html(ws, summary, prev_summary, ranking, chart_svg)

        safe_ws = ws.replace("-", "")
        report_path = out_dir / f"{safe_ws}_cfo_report.html"
        report_path.write_text(content, encoding="utf-8")

        logger.info("CFO report written to %s", report_path)
        return {
            "week_start": ws,
            "report_path": str(report_path),
        }
    finally:
        if owns_conn:
            _conn.close()


# ─── Data accessors ──────────────────────────────────────────────────────────

def _latest_week_start(conn: duckdb.DuckDBPyConnection) -> str | None:
    row = conn.execute("SELECT MAX(week_start) FROM gold_cfo_weekly_summary").fetchone()
    val = row[0] if row else None
    return str(val) if val else None


def _get_summary(conn: duckdb.DuckDBPyConnection, week_start: str) -> list[dict]:
    rows = conn.execute(f"""
        SELECT category, txn_count, amount_brl, week_end
        FROM gold_cfo_weekly_summary
        WHERE week_start = CAST('{week_start}' AS DATE)
        ORDER BY category
    """).fetchall()
    return [
        {
            "category": r[0],
            "txn_count": r[1],
            "amount_brl": float(r[2]) if r[2] is not None else 0.0,
            "week_end": str(r[3]),
        }
        for r in rows
    ]


def _get_prev_summary(conn: duckdb.DuckDBPyConnection, week_start: str) -> list[dict]:
    rows = conn.execute(f"""
        SELECT category, txn_count, amount_brl
        FROM gold_cfo_weekly_summary
        WHERE week_start = (CAST('{week_start}' AS DATE) - INTERVAL '7 days')
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


def _get_ranking(conn: duckdb.DuckDBPyConnection, week_start: str, top_n: int) -> list[dict]:
    rows = conn.execute(f"""
        SELECT merchant_id, category, txn_count, amount_brl, legal_name, trade_name
        FROM gold_cfo_weekly_merchant_ranking
        WHERE week_start = CAST('{week_start}' AS DATE)
        ORDER BY amount_brl DESC
        LIMIT {top_n}
    """).fetchall()
    return [
        {
            "merchant_id": r[0],
            "category": r[1],
            "txn_count": r[2],
            "amount_brl": float(r[3]) if r[3] is not None else 0.0,
            "legal_name": r[4] or "",
            "trade_name": r[5] or "",
        }
        for r in rows
    ]


# ─── Chart renderer ───────────────────────────────────────────────────────────

def _summary_chart(summary: list[dict]) -> str:
    """Horizontal SVG bar chart of BRL amount by category."""
    bar_h, gap = 28, 8
    label_w, value_w = 160, 140
    chart_max_w = 300
    total_brl = sum(r["amount_brl"] for r in summary) or 1.0
    svg_h = len(summary) * (bar_h + gap) + gap + 10
    svg_w = label_w + chart_max_w + value_w

    _colors = {
        "MATCHED": "#4caf50",
        "MISMATCHED": "#ff9800",
        "UNRECONCILED_PROCESSOR": "#f44336",
        "UNRECONCILED_INTERNAL": "#e91e63",
    }

    bars: list[str] = []
    for i, row in enumerate(summary):
        pct = row["amount_brl"] / total_brl
        bw = int(pct * chart_max_w)
        y = gap + i * (bar_h + gap)
        fill = _colors.get(row["category"], "#9e9e9e")
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


# ─── HTML renderer ────────────────────────────────────────────────────────────

def _render_html(
    week_start: str,
    summary: list[dict],
    prev_summary: list[dict],
    ranking: list[dict],
    chart_svg: str,
) -> str:
    week_end = summary[0]["week_end"] if summary else "—"
    total_brl = sum(r["amount_brl"] for r in summary)
    total_txn = sum(r["txn_count"] for r in summary)
    prev_total_brl = sum(r["amount_brl"] for r in prev_summary)
    wow = ((total_brl - prev_total_brl) / prev_total_brl * 100) if prev_total_brl else None
    wow_txt = f"{wow:+.1f}%" if wow is not None else "N/A"

    prev_by_cat = {r["category"]: r for r in prev_summary}

    summary_rows = "".join(
        "<tr>"
        f"<td>{html.escape(r['category'])}</td>"
        f"<td style='text-align:right'>{r['txn_count']:,}</td>"
        f"<td style='text-align:right'>R$ {r['amount_brl']:,.2f}</td>"
        f"<td style='text-align:right'>"
        + (
            f"R$ {r['amount_brl'] - prev_by_cat[r['category']]['amount_brl']:+,.2f}"
            if r["category"] in prev_by_cat
            else "—"
        )
        + "</td></tr>"
        for r in summary
    )

    ranking_rows = "".join(
        "<tr>"
        f"<td>{i}</td>"
        f"<td>{html.escape(r['trade_name'] or r['legal_name'] or r['merchant_id'])}</td>"
        f"<td>{html.escape(r['category'])}</td>"
        f"<td style='text-align:right'>{r['txn_count']:,}</td>"
        f"<td style='text-align:right'>R$ {r['amount_brl']:,.2f}</td>"
        "</tr>"
        for i, r in enumerate(ranking, 1)
    )

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <title>CFO Reconciliation Report — {week_start}</title>
  <style>
    body {{font-family:Arial,sans-serif;max-width:720px;margin:0 auto;padding:24px;color:#222}}
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
    .footer {{font-size:11px;color:#999;margin-top:36px;border-top:1px solid #eee;padding-top:10px}}
  </style>
</head>
<body>
  <h1>Weekly Reconciliation Report</h1>
  <p style="color:#666;font-size:13px">
    Week of <strong>{week_start}</strong> to <strong>{week_end}</strong>
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
    <div class="kpi">
      <div class="kpi-val">{wow_txt}</div>
      <div class="kpi-lbl">vs. Previous Week</div>
    </div>
  </div>

  <h2>Volume by Category</h2>
  {chart_svg}

  <table>
    <thead>
      <tr><th>Category</th><th>Transactions</th><th>Amount (BRL)</th><th>vs. Prev Week</th></tr>
    </thead>
    <tbody>{summary_rows}</tbody>
  </table>

  <h2>Top {len(ranking)} Merchants by Risk Amount</h2>
  <p style="font-size:12px;color:#666;margin:4px 0 8px">
    Non-matched transactions only (MISMATCHED, UNRECONCILED_PROCESSOR, UNRECONCILED_INTERNAL).
  </p>
  <table>
    <thead>
      <tr><th>#</th><th>Merchant</th><th>Category</th><th>Transactions</th><th>Amount (BRL)</th></tr>
    </thead>
    <tbody>{ranking_rows}</tbody>
  </table>

  <div class="footer">
    Data source: <code>gold_cfo_weekly_summary</code>, <code>gold_cfo_weekly_merchant_ranking</code>
    &bull; Generated automatically
  </div>
</body>
</html>"""


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    ws = sys.argv[1] if len(sys.argv) > 1 else None
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "output/reports"
    result = run(week_start=ws, output_dir=out_dir)
    print(json.dumps(result, indent=2, default=str))
