# Builds REPORTS for people from the gold warehouse:
#   data/reports/macro_quarterly.csv     complete quarters, one column per series
#   data/reports/macro_dashboard.html    a self-contained chart and summary table
#
# This DAG is scheduled on the fred_gold asset, so it runs each time
# fred_warehouse publishes a new warehouse.  fred_warehouse knows nothing
# about this DAG: a consumer can be added without touching the producer.
# The Assets view in the UI shows the whole chain:
#   fred_ingest -> fred_silver -> fred_warehouse -> fred_gold -> fred_reports
#
# The warehouse is opened read-only, so reports never block a publish.

from datetime import timedelta
from html import escape
import logging

import duckdb
import pendulum
from airflow.sdk import dag, task

from common.assets import FRED_GOLD, FRED_REPORTS
from common.medallion import (REPORTS_ROOT, WAREHOUSE_FILE, failure_alert,
                              run_sql_folder)

logger = logging.getLogger(__name__)

# Series drawn on the dashboard chart: (series_id, label, color).
CHART_SERIES = [
    ("UNRATE", "Unemployment rate", "#0E7C74"),
    ("DFF", "Fed funds rate", "#A8642A"),
    ("MORTGAGE30US", "30-yr mortgage rate", "#17212B"),
]


################################################################
# Export the wide quarterly table as CSV (SQL in sql/reports/).
@task(retries=1, execution_timeout=timedelta(minutes=5))
def export_csv() -> str:
    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE_FILE), read_only=True) as con:
        run_sql_folder(con, "reports")
    path = REPORTS_ROOT / "macro_quarterly.csv"
    logger.info("Wrote %s", path)
    return str(path)


# Draw a simple line chart as inline SVG (no plotting library needed).
def svg_line_chart(labels: list[str], series: list[tuple]) -> str:
    width, height, left, right, top, bottom = 900, 360, 50, 20, 20, 40
    plot_w, plot_h = width - left - right, height - top - bottom
    y_max = max(v for _, _, _, values in series for v in values if v is not None)
    y_max = (int(y_max) // 2 + 1) * 2  # round up to an even number
    x = lambda i: left + plot_w * i / max(len(labels) - 1, 1)
    y = lambda v: top + plot_h * (1 - v / y_max)

    parts = [f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Quarterly rates chart">']
    for tick in range(0, y_max + 1, 2):  # horizontal gridlines
        parts.append(f'<line x1="{left}" x2="{width - right}" y1="{y(tick):.1f}" y2="{y(tick):.1f}" class="grid"/>'
                     f'<text x="{left - 8}" y="{y(tick) + 4:.1f}" class="axis" text-anchor="end">{tick}%</text>')
    for i, label in enumerate(labels):  # one x label every 4 years
        if label.endswith("Q1") and int(label[:4]) % 4 == 0:
            parts.append(f'<text x="{x(i):.1f}" y="{height - 15}" class="axis" text-anchor="middle">{label[:4]}</text>')
    for _, name, color, values in series:
        points = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(values) if v is not None)
        parts.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2">'
                     f'<title>{escape(name)}</title></polyline>')
    parts.append("</svg>")
    return "".join(parts)


################################################################
# Build a self-contained HTML dashboard from the warehouse.
@task(outlets=[FRED_REPORTS], retries=1, execution_timeout=timedelta(minutes=5))
def build_dashboard() -> str:
    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE_FILE), read_only=True) as con:
        cols = ", ".join(sid for sid, _, _ in CHART_SERIES)
        rows = con.sql(f"""
            SELECT quarter_label, {cols}
            FROM macro_quarterly_wide
            WHERE quarter_key >= 20001
            ORDER BY quarter_key
        """).fetchall()
        latest = con.sql("""
            SELECT s.series_id, s.series_name, s.units_short, q.quarter_label,
                   round(f.value, 2) AS value
            FROM fact_macro_quarterly f
            JOIN dim_series  s USING (series_key)
            JOIN dim_quarter q USING (quarter_key)
            WHERE f.is_complete
            QUALIFY row_number() OVER (PARTITION BY f.series_key ORDER BY f.quarter_key DESC) = 1
            ORDER BY s.category, s.series_id
        """).fetchall()

    labels = [r[0] for r in rows]
    series = [(sid, name, color, [r[i + 1] for r in rows])
              for i, (sid, name, color) in enumerate(CHART_SERIES)]
    legend = "".join(f'<span><i style="background:{c}"></i>{escape(n)}</span>' for _, n, c, _ in series)
    table = "".join(
        f"<tr><td><code>{escape(sid)}</code></td><td>{escape(name)}</td>"
        f"<td>{escape(q)}</td><td class='num'>{v:,}</td><td>{escape(units or '')}</td></tr>"
        for sid, name, units, q, v in latest)
    built = pendulum.now("UTC").format("YYYY-MM-DD HH:mm") + " UTC"

    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FRED Macro Dashboard</title>
<style>
  body {{ font-family: Calibri, Arial, sans-serif; margin: 2rem auto; max-width: 960px; padding: 0 1rem; color: #17212B; background: #fff; }}
  h1 {{ margin-bottom: .2rem; }} .sub {{ color: #3A4A5A; margin-top: 0; }}
  svg {{ width: 100%; height: auto; }} .grid {{ stroke: #DDE2E7; }} .axis {{ font-size: 12px; fill: #3A4A5A; }}
  .legend span {{ margin-right: 1.5rem; }} .legend i {{ display: inline-block; width: 14px; height: 3px; margin-right: 6px; vertical-align: middle; }}
  .scroll {{ overflow-x: auto; }} table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
  th, td {{ border-bottom: 1px solid #DDE2E7; padding: 6px 8px; text-align: left; }} .num {{ text-align: right; }}
</style></head><body>
<h1>FRED Macro Dashboard</h1>
<p class="sub">Quarterly averages from the gold warehouse, {escape(labels[0])} to {escape(labels[-1])}. Built {built} by the fred_reports DAG.</p>
<div class="legend">{legend}</div>
{svg_line_chart(labels, series)}
<h2>Latest complete quarter, by series</h2>
<div class="scroll"><table><tr><th>Series</th><th>Name</th><th>Quarter</th><th class="num">Value</th><th>Units</th></tr>{table}</table></div>
</body></html>
"""
    path = REPORTS_ROOT / "macro_dashboard.html"
    path.write_text(html, encoding="utf-8")
    logger.info("Wrote %s (%d quarters, %d series in the table)", path, len(rows), len(latest))
    return str(path)


@dag(
    dag_id="fred_reports",
    description="Export a CSV and an HTML dashboard whenever the gold warehouse is published.",
    schedule=[FRED_GOLD],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "on_failure_callback": failure_alert,
    },
    tags=["fred", "medallion", "reports"],
)
def fred_reports():
    export_csv() >> build_dashboard()


fred_reports()
