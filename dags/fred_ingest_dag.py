# Pipeline that fetches Federal Reserve Economic Data (FRED) via the
# FRED API and organizes it using the Medallion Architecture.
#
#   BRONZE  The raw JSON returned by the API is landed unchanged,
#           partitioned by series and ingest date:
#             data/bronze/fred/series_id=GDP/ingest_date=2026-10-05/
#                 series.json         (series metadata: title, units, ...)
#                 observations.json   (the time series itself)
#                 _ingest.json        (lineage: when/what was fetched)
#
#   SILVER  DuckDB (in-process, no server) reads all bronze files and
#           writes clean, typed, deduplicated Parquet files with
#           calendar keys (YYYYMMDD, ISO week, YYYYMM, YYYYQ, YYYY).
#
# The gold layer (star schemas) is built by fred_warehouse_dag, which is
# triggered automatically when this DAG updates the silver asset.
#
# The FRED series are stratified by frequency (daily, weekly, monthly,
# quarterly, yearly).  Coarse-grained series are fetched less often to
# limit API usage; see "intervals" in config/fred_config.json.

from datetime import datetime, timedelta
import json
import logging
import os
import random

import duckdb
import pendulum
import requests
from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from common.assets import FRED_SILVER
from common.medallion import (BRONZE_ROOT, SILVER_ROOT, failure_alert,
                              load_config, run_date, run_sql_folder)

logger = logging.getLogger(__name__)

# Reference date used to stagger fetches by frequency.
EPOCH = datetime(2020, 1, 1).date()


################################################################
# Decide which series to fetch on this run.  A series is selected when:
#   - its frequency is due today (days since EPOCH % interval == 0), or
#   - it has never been fetched (no bronze folder yet), or
#   - the run was triggered with force_all = true.
# This runs as a task (not at DAG-parse time) so the decision is based
# on the run's date, which keeps reruns and backfills reproducible.
@task(retries=1, execution_timeout=timedelta(minutes=2))
def select_series() -> list[dict]:
    context = get_current_context()
    force_all = context["params"]["force_all"]
    config = load_config("fred_config.json")
    series_list = load_config("fred_series.json")["series"]
    intervals = config["intervals"]

    day_number = (run_date(context) - EPOCH).days
    due_frequencies = [freq for freq, every in intervals.items()
                       if day_number % every == 0]
    logger.info("Day %d: frequencies due = %s", day_number, due_frequencies)

    selected = []
    for s in series_list:
        never_fetched = not (BRONZE_ROOT / "fred" / f"series_id={s['id']}").exists()
        if force_all or never_fetched or s["frequency"] in due_frequencies:
            selected.append(s)
    logger.info("Selected %d of %d series: %s", len(selected),
                len(series_list), [s["id"] for s in selected])
    # An empty list means the mapped fetch task has nothing to do, so it
    # and everything downstream is skipped for this run.
    return selected


# Call one FRED API endpoint and return the raw response text.
def call_fred(endpoint: str, series_id: str, api_key: str, config: dict) -> str:
    url = f"{config['fred_api']['base_url']}/{endpoint}"
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json"}
    response = requests.get(url, params=params,
                            timeout=config["fred_api"]["timeout_seconds"])

    # 400-level errors (bad key, unknown series) will not fix themselves,
    # so fail immediately instead of wasting retries.  429 (rate limited)
    # and 500-level errors are temporary, so let Airflow retry them.
    if 400 <= response.status_code < 500 and response.status_code != 429:
        raise AirflowFailException(
            f"FRED rejected {endpoint} for {series_id} "
            f"(HTTP {response.status_code}): {response.text[:300]}")
    response.raise_for_status()
    json.loads(response.text)  # Fail now if the body is not valid JSON.
    return response.text


# Write a file atomically: write to a temp name, then rename.  A reader
# never sees a half-written file, even if the task dies mid-write.
def write_atomic(path, text: str):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


################################################################
# Dynamically mapped task: land one series' raw API responses in bronze.
# max_active_tis_per_dag limits concurrent API calls, and exponential
# backoff spaces out retries (30s, 60s, 120s, ...).
@task(
    max_active_tis_per_dag=4,
    retries=3,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=2.0,
    max_retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=5),  # Kills hung tasks.
)
def fetch_series(series: dict) -> dict:
    series_id = series["id"]
    context = get_current_context()
    config = load_config("fred_config.json")

    # Teaching aid: randomly fail to watch retries and backoff in the UI.
    faults = config.get("fault_injection", {})
    if faults.get("enabled") and random.random() < faults.get("failure_rate", 0):
        raise ConnectionError(f"Simulated network failure fetching {series_id} "
                              f"(attempt {context['ti'].try_number}).")

    # Secrets are read at run time from an Airflow Variable, never from
    # code or config files.  docker-compose sets AIRFLOW_VAR_FRED_API_KEY.
    api_key = Variable.get(config["fred_api"]["api_key_variable"])

    ingest_date = run_date(context).isoformat()
    folder = BRONZE_ROOT / "fred" / f"series_id={series_id}" / f"ingest_date={ingest_date}"
    folder.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching FRED series %s into %s ...", series_id, folder)
    series_json = call_fred("series", series_id, api_key, config)
    observations_json = call_fred("series/observations", series_id, api_key, config)

    # Land the payloads exactly as received (no parsing or reshaping).
    write_atomic(folder / "series.json", series_json)
    write_atomic(folder / "observations.json", observations_json)

    observation_count = json.loads(observations_json).get("count", 0)
    lineage = {
        "series_id": series_id,
        "ingest_date": ingest_date,
        "fetched_at": pendulum.now("UTC").isoformat(),
        "dag_run_id": context["run_id"],
        "try_number": context["ti"].try_number,
        "observation_count": observation_count,
        "bytes": len(observations_json),
    }
    write_atomic(folder / "_ingest.json", json.dumps(lineage, indent=2))
    logger.info("Landed %s: %d observations.", series_id, observation_count)
    return lineage


################################################################
# Rebuild the silver layer from ALL bronze files using DuckDB.
# The FRED_SILVER outlet tells Airflow the silver asset was updated,
# which triggers fred_warehouse (the gold layer).
# DuckDB runs in memory here (no database file), so there is no lock
# contention and the step is idempotent: rerunning it gives the same
# result.  The outlet marks the silver asset as updated on success.
@task(outlets=[FRED_SILVER], retries=1, execution_timeout=timedelta(minutes=10))
def build_silver(fetched: list[dict]) -> dict:
    logger.info("Fetched this run: %s", [f["series_id"] for f in fetched])
    (SILVER_ROOT / "fred").mkdir(parents=True, exist_ok=True)

    with duckdb.connect() as con:
        run_sql_folder(con, "silver")
        counts = {
            "series": con.sql(f"SELECT count(*) FROM '{SILVER_ROOT / 'fred' / 'series.parquet'}'").fetchone()[0],
            "observations": con.sql(f"SELECT count(*) FROM '{SILVER_ROOT / 'fred' / 'observations.parquet'}'").fetchone()[0],
        }
    logger.info("Silver row counts: %s", counts)
    return counts


################################################################
# Define the DAG.  max_active_runs=1 avoids overlapping API usage.
@dag(
    dag_id="fred_ingest",
    description="Fetch FRED series into bronze (raw JSON) and build silver (Parquet) with DuckDB.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "force_all": Param(False, type="boolean",
                           description="Fetch every series, ignoring the frequency intervals."),
    },
    default_args={
        "owner": "airflow",
        "on_failure_callback": failure_alert,
    },
    tags=["fred", "medallion", "bronze", "silver"],
)
def fred_ingest():
    series_to_fetch = select_series()
    fetched = fetch_series.expand(series=series_to_fetch)
    build_silver(fetched)


fred_ingest()
