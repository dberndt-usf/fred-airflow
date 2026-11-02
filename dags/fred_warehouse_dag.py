# Builds the GOLD layer of the FRED medallion pipeline: star schemas in
# a DuckDB database file (data/gold/warehouse.duckdb).
#
# This DAG has no clock schedule.  It is triggered by the fred_silver
# Asset, so it runs automatically each time fred_ingest refreshes silver.
#
# It follows the write-audit-publish pattern:
#   1. WRITE    build a brand-new warehouse in a staging file,
#   2. AUDIT    run data quality checks against the staging file,
#   3. PUBLISH  swap the staging file into place only if checks pass.
# Users therefore never see a half-built or failed warehouse.
#
# DuckDB allows only ONE process to open a database file for writing.
# Keeping all gold writes in a single task (and max_active_runs=1)
# avoids lock conflicts.  Close any SQL client (DBeaver, DuckDB UI)
# that has warehouse.duckdb open before this DAG publishes.

from datetime import timedelta
import logging
import os

import duckdb
import pendulum
from airflow.sdk import dag, task

from common.assets import FRED_GOLD, FRED_SILVER
from common.medallion import (GOLD_ROOT, SQL_DIR, WAREHOUSE_FILE,
                              failure_alert, render_sql, run_sql_folder,
                              sql_params)

logger = logging.getLogger(__name__)

STAGING_FILE = GOLD_ROOT / "warehouse_staging.duckdb"


################################################################
# WRITE: build every gold table from silver into a fresh staging file.
@task(retries=1, execution_timeout=timedelta(minutes=15))
def build_gold() -> dict:
    GOLD_ROOT.mkdir(parents=True, exist_ok=True)
    for leftover in (STAGING_FILE, STAGING_FILE.with_suffix(".duckdb.wal")):
        leftover.unlink(missing_ok=True)

    with duckdb.connect(str(STAGING_FILE)) as con:
        run_sql_folder(con, "gold")
        tables = dict(con.sql(
            "SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY 1"
        ).fetchall())
    for name, rows in tables.items():
        logger.info("  %-28s %8d rows", name, rows)
    return tables


################################################################
# AUDIT: each file in sql/checks returns the rows that violate a rule.
# Any returned rows fail the task (and the warehouse is not published).
# Primary/foreign keys were already enforced by DuckDB during the build.
@task(retries=0, execution_timeout=timedelta(minutes=5))
def run_checks() -> dict:
    params = sql_params()
    results, failures = {}, []
    with duckdb.connect(str(STAGING_FILE), read_only=True) as con:
        for path in sorted((SQL_DIR / "checks").glob("*.sql")):
            rel = con.sql(render_sql(path.read_text(), params))
            rows = rel.fetchall()
            results[path.stem] = "pass" if not rows else f"FAIL ({len(rows)} rows)"
            if rows:
                failures.append(path.stem)
                logger.error("Check %s FAILED. Columns %s, first rows:\n%s",
                             path.stem, rel.columns, "\n".join(map(str, rows[:10])))
            else:
                logger.info("Check %s passed.", path.stem)
    if failures:
        raise ValueError(f"Data quality checks failed: {failures}")
    return results


################################################################
# PUBLISH: atomically replace the live warehouse with the audited one.
@task(outlets=[FRED_GOLD], retries=2, retry_delay=timedelta(seconds=30))
def publish() -> str:
    os.replace(STAGING_FILE, WAREHOUSE_FILE)
    logger.info("Published %s", WAREHOUSE_FILE)
    return str(WAREHOUSE_FILE)


@dag(
    dag_id="fred_warehouse",
    description="Build FRED star schemas (gold) in DuckDB: write, audit, publish.",
    schedule=[FRED_SILVER],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "on_failure_callback": failure_alert,
    },
    tags=["fred", "medallion", "gold", "duckdb"],
)
def fred_warehouse():
    build_gold() >> run_checks() >> publish()


fred_warehouse()
