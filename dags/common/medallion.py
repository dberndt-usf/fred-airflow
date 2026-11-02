# Shared helpers for the FRED medallion pipeline:
# folder layout for the bronze/silver/gold layers, config loading,
# run-date handling, a tiny SQL-file runner for DuckDB, and alerts.

from datetime import date
from pathlib import Path
import json
import logging
import os
import re

import requests

logger = logging.getLogger(__name__)

# Folder layout.  Inside the containers the project's ./data folder is
# mounted at /opt/airflow/data.  MEDALLION_DATA_ROOT lets you run the
# same code outside Airflow (for example, to test SQL on a laptop).
DAGS_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = DAGS_DIR / "config"
SQL_DIR = DAGS_DIR / "sql"

DATA_ROOT = Path(os.environ.get("MEDALLION_DATA_ROOT", "/opt/airflow/data"))
BRONZE_ROOT = DATA_ROOT / "bronze"  # Raw API payloads, exactly as received.
SILVER_ROOT = DATA_ROOT / "silver"  # Typed, cleaned, deduplicated Parquet.
GOLD_ROOT = DATA_ROOT / "gold"      # DuckDB warehouse with star schemas.
REPORTS_ROOT = DATA_ROOT / "reports"  # Files for people: CSV and HTML.

WAREHOUSE_FILE = GOLD_ROOT / "warehouse.duckdb"


# Load a JSON file from the dags/config folder.
def load_config(filename: str) -> dict:
    with open(CONFIG_DIR / filename, "r") as f:
        return json.load(f)


# The calendar date a DAG run is "for".  In Airflow 3 a manually or
# asset-triggered run may have no logical date, so fall back to the
# time the run was scheduled to happen (run_after), which always exists.
def run_date(context) -> date:
    logical_date = context.get("logical_date")
    return (logical_date or context["dag_run"].run_after).date()


# Substitute {{ name }} placeholders in a SQL file, mimicking the Jinja
# templating that Airflow uses, so the same SQL works in any folder.
def render_sql(sql: str, params: dict) -> str:
    def replace(match):
        key = match.group(1)
        if key not in params:
            raise KeyError(f"No value supplied for SQL placeholder '{key}'.")
        return str(params[key])
    return re.sub(r"\{\{\s*(\w+)\s*\}\}", replace, sql)


# Placeholder values available to every SQL file.  Paths use forward
# slashes so the same SQL also runs on Windows.
def sql_params() -> dict:
    return {
        "bronze_root": BRONZE_ROOT.as_posix(),
        "silver_root": SILVER_ROOT.as_posix(),
        "gold_root": GOLD_ROOT.as_posix(),
        "reports_root": REPORTS_ROOT.as_posix(),
        "config_dir": CONFIG_DIR.as_posix(),
    }


# Run every .sql file in a folder (in name order) on a DuckDB connection.
def run_sql_folder(con, folder: str) -> list[str]:
    files = sorted((SQL_DIR / folder).glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No SQL files found in {SQL_DIR / folder}")
    params = sql_params()
    for path in files:
        logger.info("Running %s ...", path.relative_to(SQL_DIR))
        con.execute(render_sql(path.read_text(), params))
    return [p.name for p in files]


# Post a message to Slack if a webhook is configured, otherwise just log.
# The webhook comes from the Airflow Variable slack_webhook_url, which the
# docker-compose file fills from SLACK_WEBHOOK_URL in the .env file.
def notify(message: str):
    from airflow.sdk import Variable
    webhook = Variable.get("slack_webhook_url", default=None)
    if not webhook:
        logger.info("Slack disabled.  Message: %s", message)
        return
    try:
        response = requests.post(webhook, json={"text": message}, timeout=5)
        if response.status_code != 200:
            logger.warning("Slack returned error %d: %s",
                           response.status_code, response.text)
    except Exception as e:
        logger.error("Slack notification failed: %s", e)


# Task failure callback (set in each DAG's default_args).
def failure_alert(context):
    ti = context["ti"]
    notify(
        ":rotating_light: *Airflow task failed*\n"
        f"*DAG:* {ti.dag_id}\n*Task:* {ti.task_id}\n"
        f"*Run:* {ti.run_id}\n*Log:* {getattr(ti, 'log_url', 'n/a')}"
    )
