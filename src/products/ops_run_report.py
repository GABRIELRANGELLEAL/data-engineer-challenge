"""
Ops run history report producer.

Renders an HTML report of every reconciliation run attempt (COMPLETED or not)
for the 7-day window ending on a reference_date, sourced from gold_ops_run_history —
which, unlike gold_ops_reconciliation_daily, does not apply a winning-run filter
and so also surfaces reruns and runs that failed before producing any results.

No alert message and no threshold evaluation here — this is an audit/visibility
report, not a Slack-style alert (that's ops_alert.py's job).

Writes to: {output_dir}/{reference_date}_ops_run_report.html
"""
import html
import logging
from datetime import date, timedelta
from pathlib import Path

import duckdb

from src.db import get_connection

logger = logging.getLogger(__name__)

_STATUS_COLORS = {
    "COMPLETED": "#4caf50",
    "FAILED": "#f44336",
    "RUNNING": "#ff9800",
}

_WINDOW_DAYS = 7


def run(
    reference_date: str | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
    output_dir: str | Path = "outputs",
) -> dict:
    """
    Renders the run-history report covering the 7 days before reference_date, plus
    reference_date itself (8 days total).

    Args:
        reference_date: Date to evaluate (YYYY-MM-DD). Defaults to the latest
                         reference_date available in gold_ops_run_history.
        output_dir:      Directory where the report file is written.

    Returns:
        Dict with reference_date, start_date, run_count, and report_path.
    """
    owns_conn = conn is None
    _conn = conn if conn is not None else get_connection()

    try:
        ref_date = reference_date or _latest_reference_date(_conn)
        if ref_date is None:
            logger.warning("No run history found — nothing to report.")
            return {"status": "no_data"}

        start_date = str(date.fromisoformat(ref_date) - timedelta(days=_WINDOW_DAYS))
        runs = _runs_for_dates(_conn, start_date, ref_date)

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        content = _render_html(ref_date, start_date, runs)

        report_path = out_dir / f"{ref_date}_ops_run_report.html"
        report_path.write_text(content, encoding="utf-8")

        logger.info(
            "Ops run report written — reference_date=%s start_date=%s runs=%d path=%s",
            ref_date, start_date, len(runs), report_path,
        )
        return {
            "reference_date": ref_date,
            "start_date": start_date,
            "run_count": len(runs),
            "report_path": str(report_path),
        }
    finally:
        if owns_conn:
            _conn.close()


# ─── Data accessors ──────────────────────────────────────────────────────────

def _latest_reference_date(conn: duckdb.DuckDBPyConnection) -> str | None:
    row = conn.execute(
        "SELECT MAX(reference_date) FROM gold_ops_run_history"
    ).fetchone()
    val = row[0] if row else None
    return str(val) if val else None


def _runs_for_dates(
    conn: duckdb.DuckDBPyConnection, start_date: str, end_date: str
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            run_id, reference_date, file_name, run_status, total_transactions,
            CAST(started_at AS VARCHAR)   AS started_at,
            CAST(completed_at AS VARCHAR) AS completed_at,
            category, txn_count, pct_of_total
        FROM gold_ops_run_history
        WHERE reference_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ORDER BY reference_date DESC, started_at DESC, run_id, category
        """,
        [start_date, end_date],
    ).fetchall()

    runs_by_id: dict[int, dict] = {}
    order: list[int] = []
    for r in rows:
        run_id = r[0]
        if run_id not in runs_by_id:
            runs_by_id[run_id] = {
                "run_id": run_id,
                "reference_date": str(r[1]),
                "file_name": r[2],
                "run_status": r[3],
                "total_transactions": r[4],
                "started_at": r[5],
                "completed_at": r[6],
                "categories": [],
            }
            order.append(run_id)
        if r[7] is not None:
            runs_by_id[run_id]["categories"].append({
                "category": r[7],
                "txn_count": r[8],
                "pct_of_total": float(r[9]) if r[9] is not None else 0.0,
            })

    return [runs_by_id[run_id] for run_id in order]


# ─── HTML renderer ────────────────────────────────────────────────────────────

def _render_html(ref_date: str, start_date: str, runs: list[dict]) -> str:
    by_date: dict[str, list[dict]] = {}
    for r in runs:
        by_date.setdefault(r["reference_date"], []).append(r)

    window_dates = [
        str(date.fromisoformat(start_date) + timedelta(days=i))
        for i in range(_WINDOW_DAYS, -1, -1)
    ]
    sections = "".join(
        _render_date_section(d, by_date.get(d, []))
        for d in window_dates
    )

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <title>Ops Run History Report — {ref_date}</title>
  <style>
    body {{font-family:Arial,sans-serif;max-width:960px;margin:0 auto;padding:24px;color:#222}}
    h1  {{font-size:20px;border-bottom:2px solid #1a73e8;padding-bottom:8px;margin-bottom:4px}}
    h2  {{font-size:14px;color:#444;margin-top:28px;text-transform:uppercase;letter-spacing:.05em}}
    table {{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
    th {{background:#f0f4ff;text-align:left;padding:7px 10px;border-bottom:2px solid #c5d0e6}}
    td {{padding:6px 10px;border-bottom:1px solid #edf0f8}}
    .status-pill {{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;
                    color:#fff;font-weight:bold}}
    .no-runs {{color:#999;font-size:13px;margin-top:8px}}
    .footer {{font-size:11px;color:#999;margin-top:36px;border-top:1px solid #eee;padding-top:10px}}
  </style>
</head>
<body>
  <h1>Ops Run History Report</h1>
  <p style="color:#666;font-size:13px">
    <strong>{start_date}</strong> to <strong>{ref_date}</strong> (last {_WINDOW_DAYS + 1} days)
    &bull; Every run attempt, regardless of outcome &bull; Generated automatically
  </p>
  {sections}
  <div class="footer">
    Data source: <code>gold_ops_run_history</code> &bull; Generated automatically
  </div>
</body>
</html>"""


def _render_date_section(day: str, day_runs: list[dict]) -> str:
    if not day_runs:
        return f"<h2>{day}</h2><p class='no-runs'>No run attempts recorded for this date.</p>"

    rows = "".join(_render_run_row(r) for r in day_runs)
    return f"""
  <h2>{day}</h2>
  <table>
    <thead>
      <tr>
        <th>Run ID</th><th>File</th><th>Status</th><th>Started</th><th>Completed</th>
        <th>Total Txns</th><th>Category Breakdown</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>"""


def _render_run_row(r: dict) -> str:
    color = _STATUS_COLORS.get(r["run_status"], "#9e9e9e")
    if r["categories"]:
        breakdown = "; ".join(
            f"{html.escape(c['category'])}: {c['txn_count']:,} ({c['pct_of_total']:.1%})"
            for c in r["categories"]
        )
    else:
        breakdown = "<em>no results</em>"

    return (
        "<tr>"
        f"<td>{r['run_id']}</td>"
        f"<td>{html.escape(r['file_name'] or '')}</td>"
        f"<td><span class='status-pill' style='background:{color}'>"
        f"{html.escape(r['run_status'] or '')}</span></td>"
        f"<td>{r['started_at']}</td>"
        f"<td>{r['completed_at'] if r['completed_at'] is not None else '—'}</td>"
        f"<td style='text-align:right'>{r['total_transactions']:,}</td>"
        f"<td>{breakdown}</td>"
        "</tr>"
    )


if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    ref = sys.argv[1] if len(sys.argv) > 1 else None
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "outputs"
    result = run(reference_date=ref, output_dir=out_dir)
    print(json.dumps(result, indent=2, default=str))
