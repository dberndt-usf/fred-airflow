# Student Guide: Orchestrating the Medallion

This guide goes with the teaching case *Orchestrating the Medallion: An
Airflow 3 and DuckDB Teaching Case*. The case tells the story of a small
research group that rebuilds a failing data pipeline. This repository is
that pipeline. The guide connects the two: it defines the terms the case
uses, shows where each part of the story lives in the code, and helps you
prepare for the class discussion.

## How to use this guide

| When | Do this | Time |
| --- | --- | --- |
| Before class | Read the case. Skim [Key concepts](#1-key-concepts) for terms you do not know. | 60 min |
| Before class | Install and start the pipeline ([Running Airflow](../README.md#running-airflow)). | 30 min |
| Before class | Read the code in the order of [Reading the code](#3-reading-the-code). | 45 min |
| In class | Work through the [lab handout](lab_handout.md). | 100 min |
| After class | Prepare the discussion questions using [Preparing for discussion](#4-preparing-for-discussion). | 60 min |

You do not need to know Airflow or DuckDB before you start. You do need
basic SQL (joins and `GROUP BY`) and enough Python to read a function.

## 1 Key concepts

### Orchestration

| Term | Meaning | In this repository |
| --- | --- | --- |
| Scheduler vs. orchestrator | A scheduler starts jobs at set times. An orchestrator also tracks dependencies, retries failures, records every run, and shows what happened. | Airflow is the orchestrator. |
| DAG | A directed acyclic graph of tasks: a workflow with no loops, defined in Python. | `dags/fred_*_dag.py` |
| Task | One step in a DAG. With the TaskFlow API, a Python function decorated with `@task`. | `select_series`, `fetch_series`, `build_silver`, … |
| DAG run / task instance | One execution of a DAG, and one execution of a task within it. Each has a state: queued, running, success, failed, skipped. | the Grid view |
| Dynamic task mapping | Creating one task instance per item in a list at run time with `.expand()`. | `fetch_series.expand(series=...)` |
| Asset | A named piece of data. A task that updates it lists it in `outlets`; a DAG scheduled on it runs when it is updated. Called a *dataset* before Airflow 3. | `dags/common/assets.py` |
| Param | A value set when a run is triggered by hand, stored with the run. | `force_all` |
| Variable | A named configuration value or secret. Here supplied as `AIRFLOW_VAR_*` environment variables from `.env`. | the FRED API key |
| Retry and backoff | Running a failed task again after a wait that grows each time. | `fetch_series`: 3 retries, 30 s, 60 s, 120 s |
| Fail fast | Failing at once, without retrying, on an error that cannot fix itself. | HTTP 4xx other than 429 raises `AirflowFailException` |
| Parse time vs. run time | Code at the top of a DAG file runs every time Airflow parses the file. Code inside a task runs only when the task runs. Decisions belong at run time. | `select_series` decides inside a task |

### Airflow's components

| Component | Job | Container |
| --- | --- | --- |
| Metadata database | Stores DAGs, runs, task states, and asset events | `postgres` |
| DAG processor | Reads and parses the files in `dags/` | `airflow-dag-processor` |
| Scheduler | Decides what should run and hands it to the executor | `airflow-scheduler` |
| Executor | Runs the tasks. `LocalExecutor` runs them as processes in the scheduler's container. | (inside the scheduler) |
| API server | Serves the web UI, the REST API, and the API that running tasks report to | `airflow-apiserver` |

### Data architecture

| Term | Meaning | In this repository |
| --- | --- | --- |
| Medallion Architecture | Three layers of increasing refinement: bronze (raw), silver (clean), gold (modeled). | `data/bronze`, `data/silver`, `data/gold` |
| Schema-on-read / schema-on-write | Applying a structure when data is queried, versus enforcing it when data is stored. | bronze is read with a schema; gold enforces one |
| Hive-style partitioning | Folders named `key=value`, so tools can treat a folder tree as a table and skip folders a query does not need. Comes from Apache Hive. | `series_id=UNRATE/ingest_date=2026-11-02/` |
| Parquet | A compressed, column-oriented file format for analytic data. | `data/silver/fred/*.parquet` |
| In-process database | A database engine that runs inside your program rather than as a server. | DuckDB |
| Single writer | A DuckDB file can have many readers but only one writer at a time. | always open the warehouse with `-readonly` |
| Lineage | A record of where data came from and how it was produced. | `_ingest.json` in each bronze folder |
| Idempotent | Running the same step twice gives the same result as running it once. | silver and gold are rebuilt from bronze |
| Write-audit-publish | Build new data in a staging area, check it, and only then make it live. | `fred_warehouse`: `build_gold`, `run_checks`, `publish` |

### Dimensional modeling

| Term | Meaning | In this repository |
| --- | --- | --- |
| Fact table | Measurements, one row per event or observation, with keys to dimensions. | `fact_daily_observation`, … |
| Dimension table | Descriptive context for facts: who, what, when. | `dim_series`, `dim_date` |
| Star schema | A fact table joined directly to its dimensions. | each `fact_*` table and its dimensions |
| Grain | What one row of a fact table represents. | one series on one day, week, month, quarter, or year |
| Conformed dimension | A dimension shared by several fact tables, so they can be compared. | `dim_date`, `dim_month`, `dim_quarter`, `dim_year`, `dim_series` |
| Drill-across | Combining facts of different grains on a shared dimension. | `fact_macro_quarterly` puts every series on `quarter_key` |
| Calendar keys | Integer keys that encode a period: YYYYMMDD, YYYYMM, YYYYQ. | `date_key`, `month_key`, `quarter_key` (2026 Q3 = `20263`) |
| Aggregation method | How a finer series rolls up to a coarser period: average, end of period, or sum. | `aggregation_method` in `fred_series.json` |
| Slowly changing dimension | A dimension attribute that changes over time, such as a company's sector. Type 1 overwrites it; Type 2 keeps a row per version. | not yet: README Exercise 3, and case discussion question 11 |

## 2 Where the case meets the code

| Case section | What it describes | Look at |
| --- | --- | --- |
| 2 Legacy pipeline | the old Airflow 2 and Oracle design | the case text only; section 2.2 lists the problems this repository fixes |
| 3.2 Apache Hive | partition folders, schema-on-read | `data/bronze/fred/`, and the `read_json(... hive_partitioning = true)` call in `dags/sql/silver/` |
| 4 Redesign | six decisions | `dags/config/`, `.env.example`, `docker-compose.yaml` |
| 5.1 Bronze | fetching and landing raw JSON | `fetch_series` and `call_fred` in `dags/fred_ingest_dag.py` |
| 5.2 Silver | cleaning, typing, calendar keys | `dags/sql/silver/02_observations.sql` |
| 5.3 Gold | fact-table options, star schemas | `dags/sql/gold/01` to `06`, especially `04_fact_observations.sql` (one table per frequency) |
| 5.4 Reports | a new consumer of an existing asset | `dags/fred_reports_dag.py`, `dags/common/assets.py` |
| 6.1 Throttling | most days nothing is fetched | `select_series`, `intervals` in `dags/config/fred_config.json` |
| 6.2 Retries | retry, backoff, fail fast, fault injection | the `@task(...)` settings on `fetch_series` |
| 6.3 Write-audit-publish | staging, checks, atomic swap | `dags/fred_warehouse_dag.py`, `dags/sql/checks/` |
| 6.4 Paused consumer | missed asset events | the Assets page in the UI; lab question Q19 |

## 3 Reading the code

Read in this order. Each file is short, and each step builds on the last.

1. **`dags/config/fred_series.json` and `fred_config.json`.** What the
   pipeline fetches and how often. Notice that there are no secrets here.
2. **`dags/common/medallion.py`.** Shared helpers: where each layer lives,
   how a run's date is found, and how a folder of SQL files is run in
   order.
3. **`dags/fred_ingest_dag.py`.** Start at the `@dag` function at the
   bottom and read upward. Ask, for each task: when does this code run,
   what does it read, what does it write?
4. **`dags/sql/silver/02_observations.sql`.** The heart of the cleaning.
   Find the `QUALIFY` clause and work out what it keeps.
5. **`dags/fred_warehouse_dag.py`** and **`dags/sql/checks/`.** Find the
   three steps of write-audit-publish. What happens to the live warehouse
   if a check returns rows?
6. **`dags/sql/gold/05_fact_macro_quarterly.sql`.** The drill-across fact.
   Find where `aggregation_method` is used and where `is_complete` is set.
7. **`dags/fred_reports_dag.py`.** A complete consumer in about 150 lines.
   Why does it open the warehouse read-only?
8. **Case section 2.2 again.** For each problem in the legacy pipeline,
   name the file in this repository that addresses it.

Then query each layer yourself with
[`analysis/demo_queries.sql`](../analysis/demo_queries.sql).

## 4 Preparing for discussion

The case ends with eleven discussion questions. These prompts point you to
evidence; they do not give answers. Bring specific file names, query
results, or screenshots to class.

1. **The quiet failure.** List the legacy problems in case section 2.2.
   For each, find the line or setting in the new code that addresses it.
   Is any problem not fully solved?
2. **Scheduler vs. orchestrator.** Pick three things you saw in the
   Airflow UI during the lab. How would you get each one with cron and
   shell scripts?
3. **Schema-on-read.** Query `observations.json` in bronze directly with
   DuckDB (lab Part C). Where did a schema get applied, and by whom?
4. **Keeping every response.** Count the `ingest_date` folders for one
   series. Estimate the disk used by bronze after five years. Then think
   of a question only the old responses can answer.
5. **Calendar keys.** Compare `obs_date`, `period_start_date`, and
   `period_end_date` for a weekly and a monthly series (demo query 2d).
6. **Fact-table options and aggregation methods.** Sketch the three
   options in the case's Table 3 for two series of different frequencies,
   say DFF and UNRATE. Write one query against each design that compares
   them by quarter, and note what could go wrong. Then run demo query 3f,
   decide which aggregation method fits each series in
   `fred_series.json`, and check yours against the configuration.
7. **Assets.** Open the Assets page. Trace one update from `fred_ingest`
   to `fred_reports`. What would you have to change to add a fourth DAG
   that consumes `fred_gold`?
8. **Write-audit-publish.** Do the "Fail a data quality check" exercise
   in the [README](../README.md#things-to-try). Before and after, note the
   modification time of `data/gold/warehouse.duckdb`. Then invent an
   error that all four checks would miss.
9. **Single writer.** Find every place the pipeline opens a DuckDB file,
   and note which ones write. Why does `build_gold` write to a staging
   file rather than the live warehouse? What would an analyst holding
   the warehouse open for writing block?
10. **The decision ahead.** For each of Sam's three paths, list which
    files in this repository would change and which would survive. Look
    up the cost of one managed Airflow service and one cloud data
    warehouse for a team of six.
11. **Modeling stock prices.** Look up the 2018 GICS reclassification
    and list three companies that changed sector. For each of the three
    stock designs in case section 7, describe what happens to those
    companies' price history. Then write the two questions a sector
    analyst might ask that need the sector *at the time of trading* and
    the *current* sector.

## 5 Going further

- The [Exercises](../README.md#exercises) in the README extend the
  pipeline: incremental loads, stable surrogate keys, a Type 2 slowly
  changing dimension, a trading calendar, and a second data source.
- Rebuild silver or gold by hand: stop Airflow, then run the SQL files in
  `dags/sql/` from the DuckDB CLI in order. What did Airflow add that you
  had to do yourself?
- Compare orchestrators: rewrite `fred_reports` as a Dagster asset or a
  Prefect flow, and compare the code.

## Further reading

- Apache Airflow documentation: <https://airflow.apache.org/docs/>
  (start with "Core Concepts" and "Asset Definitions").
- DuckDB documentation: <https://duckdb.org/docs/> (see "Hive
  Partitioning" and "Reading Multiple Files").
- Databricks, "What is the medallion lakehouse architecture?":
  <https://docs.databricks.com/aws/en/lakehouse/medallion>
- Kimball, R., & Ross, M. (2013). *The Data Warehouse Toolkit* (3rd ed.).
  Wiley. Chapters 1 to 3 cover facts, dimensions, and grain.
- Thusoo, A., et al. (2009). Hive: A warehousing solution over a
  map-reduce framework. *Proceedings of the VLDB Endowment, 2*(2).
- Armbrust, M., Ghodsi, A., Xin, R., & Zaharia, M. (2021). Lakehouse: A
  new generation of open platforms that unify data warehousing and
  advanced analytics. *CIDR 2021*.
- FRED API documentation: <https://fred.stlouisfed.org/docs/api/fred/>
