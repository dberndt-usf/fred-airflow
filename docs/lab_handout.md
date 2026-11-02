# Lab: Running the FRED Medallion Pipeline

October 5, 2026

## Overview

In this lab you run a two-DAG Apache Airflow 3 pipeline that fetches 16 economic time series from FRED, lands the raw JSON (bronze), cleans it into Parquet (silver), and builds star schemas in DuckDB (gold). Then you break it on purpose and query the result. Plan on about 100 minutes.

By the end you should be able to:

1. Start Airflow with Docker Compose and find your way around the UI.
2. Explain how dynamic task mapping, retries, and assets show up in a real run.
3. Trace one data value from the raw API response to the gold warehouse.
4. Tell a temporary failure from a permanent one, and a constraint violation from a failed data quality check.
5. Write star-join and drill-across SQL against the warehouse.

Questions are numbered **Q1** to **Q20** as you go. Write your answers in a separate document; the submission checklist is at the end. Your instructor may also assign the optional **Part G** (about 30 minutes, questions Q21 to Q24), in which you add a new economic indicator yourself.

## Before you start

You need four things installed or ready before the lab. Docker does the heavy lifting; everything else is small.

| Item | Why | Get it |
| --- | --- | --- |
| Docker Desktop, 4 GB+ memory | runs Airflow and its Postgres metadata database | [docker.com](https://www.docker.com/products/docker-desktop/) |
| FRED API key (free) | the pipeline calls the FRED API | [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) |
| DuckDB CLI 1.4 or later | you query bronze, silver, and gold from your laptop | `winget install DuckDB.cli` (Windows) or `brew install duckdb` (macOS); see [duckdb.org](https://duckdb.org/docs/installation/) |
| A text editor | you edit two config files | any |

If you cannot install the DuckDB CLI, the Airflow image includes a copy. Put `docker compose exec airflow-scheduler` in front of any `duckdb` command in this lab, for example `docker compose exec airflow-scheduler duckdb -readonly data/gold/warehouse.duckdb`.

Set up the project folder:

1. Unzip or clone the `airflow-poc` folder and open a terminal in it. Every command in this handout runs from that folder.
2. Copy `.env.example` to `.env` (Windows: `copy .env.example .env`; macOS/Linux: `cp .env.example .env`).
3. Open `.env` and paste your key after `FRED_API_KEY=`. Leave `SLACK_WEBHOOK_URL` blank. Never share or submit this file.

What is in the folder:

| Path | Contents |
| --- | --- |
| `dags/fred_ingest_dag.py` | DAG 1: fetch (bronze) and clean (silver) |
| `dags/fred_warehouse_dag.py` | DAG 2: build, check, and publish the gold warehouse |
| `dags/fred_reports_dag.py` | DAG 3: a CSV and an HTML dashboard from the warehouse (Part F) |
| `dags/config/` | which series to fetch, fetch intervals, fault injection switch |
| `dags/sql/` | the silver, gold, data quality, and report SQL |
| `data/` | bronze, silver, gold, and reports, created by the pipeline |
| `analysis/example_queries.sql` | example queries for Part E |

## Part A: Start Airflow and tour the UI (15 min)

Airflow runs as a few cooperating containers; you start them with one command and then explore before running anything.

1. Start the stack. The first build downloads images and takes 3 to 5 minutes.

   ```bash
   docker compose up -d --build
   ```
2. Check the containers. Wait until each long-running service shows `(healthy)`; `airflow-init` should show `Exited (0)` because it only sets things up.

   ```bash
   docker compose ps -a
   ```
3. Open <http://localhost:8080> and log in as `admin` / `admin`.
4. On the **DAGs** page, find the four DAGs. All start paused; leave fred_reports paused until Part F.
5. Open **fred_ingest**, then its **Code** tab, and skim the file.
6. Press `g` (or use the toggle above the grid) to switch between **Grid** and **Graph** views.

**Q1.** Which container parses the files in `dags/`, and which one runs your task code? (Hint: the executor is `LocalExecutor`.)

**Q2.** What is the schedule of `fred_ingest`? `fred_warehouse` has no time-based schedule: what triggers it?

**Q3.** From the graph view, list the tasks of each DAG in the order they run.

## Part B: Run the pipeline (20 min)

Unpausing the two DAGs is all it takes: the first run fetches every series, and the warehouse builds itself when silver is ready.

1. Unpause **fred_warehouse** first (the toggle on the DAGs page). It will wait for the silver asset.
2. Unpause **fred_ingest**. Airflow immediately starts a scheduled run for today. Because `data/bronze` is empty, `select_series` picks all 16 series.
3. Open the run in the **Grid** view and click `fetch_series [16]` to see the 16 mapped task instances.
4. Open the log of the `fetch_series` instance for `DFF` and find the line `Landed DFF: … observations.`
5. When `build_silver` succeeds, open **Assets**, then `fred_silver`. Follow the link to the `fred_warehouse` run it triggered.
6. In that run, open the `run_checks` log and find the four `Check … passed.` lines.
7. Back on **fred_ingest**, click **Trigger** and trigger a run with the default parameters (`force_all` off).

**Q4.** How many `fetch_series` instances ran, and how long did the slowest one take?

**Q5.** How many observations does `DFF` have? Which year does it start in?

**Q6.** What does the `fred_warehouse` run ID start with, and what does that tell you about how it was started?

**Q7.** In the run you triggered in step 7, which tasks ran and which were skipped? Explain why, using the `intervals` setting in `dags/config/fred_config.json` and the `select_series` log.

## Part C: Explore bronze and silver (20 min)

Bronze and silver are plain files, so you can query them directly with DuckDB while Airflow keeps running.

1. In `data/bronze/fred/series_id=UNRATE/`, open the `ingest_date=…` folder and look at `_ingest.json` and the start of `observations.json`.
2. Start DuckDB in memory (no database file) from the project folder:

   ```bash
   duckdb
   ```
3. Read the raw JSON from every bronze folder at once:

   ```sql
   SELECT series_id, ingest_date, count, units
   FROM read_json('data/bronze/fred/series_id=*/ingest_date=*/observations.json',
                  hive_partitioning = true,
                  columns = {'count': 'INTEGER', 'units': 'VARCHAR'})
   ORDER BY count DESC;
   ```
4. Find the missing values in silver since 2025:

   ```sql
   SELECT series_id, count(*) AS missing_days, min(obs_date), max(obs_date)
   FROM 'data/silver/fred/observations.parquet'
   WHERE is_missing AND obs_date >= '2025-01-01'
   GROUP BY series_id;
   ```
5. Compare how weekly, monthly, and quarterly series are dated:

   ```sql
   SELECT series_id, obs_date, dayname(obs_date) AS weekday,
          period_start_date, period_end_date, month_key, quarter_key
   FROM 'data/silver/fred/observations.parquet'
   WHERE (series_id = 'WM2NS' AND obs_date >= '2026-08-01')
      OR (series_id IN ('UNRATE', 'GDP') AND obs_date >= '2026-01-01')
   ORDER BY series_id, obs_date;
   ```

**Q8.** What is in `_ingest.json`, and why is it useful? Is the API key in it?

**Q9.** Which series have missing values since 2025? Look up two of the missing dates for `SP500` or `DGS10`. Why are they missing? Why are `UNRATE` and `CPILFESL` both missing October 2025?

**Q10.** On which weekday are `WM2NS` observations dated? Does that date mark the start or the end of the week? Why do the rollup keys (`month_key`, `quarter_key`) come from `period_end_date`?

**Optional: see deduplication happen.** Every fetch lands the full history again, and silver keeps only the newest ingest per series and date.

1. Copy the folder `data/bronze/fred/series_id=GDP/ingest_date=<today>` to a sibling folder named `ingest_date=2030-01-01`.
2. In the copy's `observations.json`, change the value for `2026-04-01` to `99999`.
3. Trigger **fred_ingest** with `force_all` switched on, and wait for `build_silver` to finish.
4. Query silver for GDP on `2026-04-01`.
5. Delete the `ingest_date=2030-01-01` folder and trigger again with `force_all` on.

**Q11.** Which GDP value did silver keep in step 4, and why? What would have happened if you had named the folder `ingest_date=2020-01-01` instead?

## Part D: Break it on purpose (20 min)

Each experiment changes one config file, triggers a run, and shows a different line of defense. The tasks read their config at run time, so no restart is needed. **Undo each change before starting the next experiment.**

### D1. Temporary failures: retries with backoff

1. In `dags/config/fred_config.json`, set `"enabled": true` under `fault_injection`. Each fetch attempt now fails 30% of the time.
2. Trigger **fred_ingest** with `force_all` switched on.
3. In the Grid view, watch `fetch_series` instances turn yellow (*up for retry*) and then green.
4. Open an instance whose **Try Number** is 2 or more. The **Task Tries** buttons above the log show one log per try.
5. Set `"enabled": false` again.

**Q12.** How many instances needed at least one retry? For one of them, about how long was the wait before each retry? Compare with `retry_delay` and `retry_exponential_backoff` in `fred_ingest_dag.py`.

**Q13.** Each series gets 4 tries (1 + 3 retries). With a 30% failure rate per try, what is the probability that one series fails all 4? That at least one of the 16 does? What happens to `build_silver` if that occurs?

### D2. Permanent failures: fail fast

1. Add this entry to the `series` list in `dags/config/fred_series.json` (mind the commas):

   ```json
   {"id": "NOTAREALSERIES", "name": "Bogus", "category": "Test",
    "frequency": "monthly", "aggregation_method": "avg"}
   ```
2. Trigger **fred_ingest** with default parameters. The new series has never been fetched, so it is selected automatically.
3. Read the failed `fetch_series` log, then check `build_silver` and **fred_warehouse**.
4. Remove the entry.

**Q14.** How many tries did the bogus series get, and why not 4? Which line in `call_fred` decides that? What state is `build_silver` in, and did `fred_warehouse` run?

### D3. Bad configuration: constraints versus checks

1. In `fred_series.json`, change **GDP**'s frequency from `quarterly` to `monthly`. Trigger **fred_ingest** with `force_all` on, then open the **fred_warehouse** run it triggers.
2. Change GDP back to `quarterly`. Now change **UNRATE**'s frequency from `monthly` to `quarterly`. Trigger again with `force_all` on and open the new **fred_warehouse** run.
3. Change UNRATE back to `monthly` and trigger once more with `force_all` on. Confirm everything is green.

**Q15.** Which task failed in step 1, and which check caught the problem? Which task failed in step 2, and what error did it report? Explain why the UNRATE change never reached `run_checks`. (Hint: think about how many monthly rows land in one quarter and the `PRIMARY KEY` of `fact_quarterly_observation`.)

**Q16.** In both cases, was `data/gold/warehouse.duckdb` changed? Which design choice protects it?

## Part E: Query the gold warehouse (15 min)

The gold layer is one DuckDB file holding star schemas. Always open it read-only so Airflow can still publish new builds while you work.

1. Exit the in-memory session from Part C (`.quit`) and open the warehouse:

   ```bash
   duckdb -readonly data/gold/warehouse.duckdb
   ```
2. List the tables and inspect the quarterly fact table:

   ```sql
   SHOW TABLES;
   DESCRIBE fact_macro_quarterly;
   ```
3. Run a star join at native grain (monthly unemployment, averaged by year):

   ```sql
   SELECT m.year_key, round(avg(f.value), 2) AS avg_unemployment_rate
   FROM fact_monthly_observation f
   JOIN dim_month  m USING (month_key)
   JOIN dim_series s USING (series_key)
   WHERE s.series_id = 'UNRATE' AND m.year_key >= 2015
   GROUP BY m.year_key
   ORDER BY m.year_key;
   ```
4. Run a drill-across query: series of different frequencies side by side on `quarter_key`:

   ```sql
   SELECT q.quarter_label,
          max(f.value) FILTER (WHERE s.series_id = 'GDPC1')  AS real_gdp,
          max(f.value) FILTER (WHERE s.series_id = 'UNRATE') AS unemployment,
          max(f.value) FILTER (WHERE s.series_id = 'DFF')    AS fed_funds
   FROM fact_macro_quarterly f
   JOIN dim_quarter q USING (quarter_key)
   JOIN dim_series  s USING (series_key)
   WHERE f.is_complete AND q.year_key >= 2022
   GROUP BY q.quarter_key, q.quarter_label
   ORDER BY q.quarter_key;
   ```
5. Write your own query for Q17. More examples are in `analysis/example_queries.sql`.

**Q17.** Write a query that finds the complete quarter since 2000 with the highest average 30-year mortgage rate (`MORTGAGE30US`), and shows the fed funds rate (`DFF`) for the same quarter. Submit the SQL and its result.

**Q18.** Run query 6 in `analysis/example_queries.sql` (three ways to roll up M2). Which method does the warehouse use for M2, and why is summing weekly M2 values meaningless? Name one series in the warehouse where `sum` is the right choice.

## Part F: Follow the data downstream (10 min)

A third DAG, `fred_reports`, is scheduled on the `fred_gold` asset. It turns the warehouse into files for people: a CSV and an HTML dashboard in `data/reports/`. Nothing in `fred_warehouse` refers to it.

1. On the **DAGs** page, unpause **fred_reports**. Wait a minute and check whether it runs.
2. Trigger **fred_ingest** with `force_all` switched on. Watch the chain: `fred_ingest`, then `fred_warehouse`, then `fred_reports`.
3. Open `data/reports/macro_dashboard.html` in a web browser, and `data/reports/macro_quarterly.csv` in a spreadsheet.
4. Open **Assets**, then `fred_gold`. Read the lineage graph and the **Asset Events** list beside it.
5. Open `dags/fred_reports_dag.py` and find its `schedule` and the `outlets` of `build_dashboard`.

**Q19.** Did `fred_reports` run when you unpaused it in step 1? In the `fred_gold` asset events, which events are listed for the `fred_reports` run from step 2, including any from before you unpaused it? What does this tell you about consumers that are paused when their asset updates?

**Q20.** Adding `fred_reports` required no change to `fred_warehouse`. Why is that useful when several teams consume the same data? If `run_checks` fails, as in experiment D3, does `fred_reports` run? Explain using the `publish` task.

## Part G (optional): Add an economic indicator (30 min)

The pipeline is driven by configuration. In this part you add a 17th series and prove, with evidence, that it travelled through every layer. Do Part G after Parts A to F, with all three FRED DAGs unpaused.

### G1. Choose a series

Pick a FRED series that is not already in `dags/config/fred_series.json`. Avoid annual series: they are kept at their own grain and do not appear in the quarterly tables you will check in G4. Some suggestions:

| Series | What it measures | Frequency |
| --- | --- | --- |
| `HOUST` | housing starts | monthly |
| `ICSA` | initial claims for unemployment insurance | weekly |
| `INDPRO` | industrial production index | monthly |
| `JTSJOL` | job openings | monthly |
| `UMCSENT` | consumer sentiment index | monthly |
| `T10Y2Y` | 10-year minus 2-year Treasury yield spread | daily |
| `DCOILWTICO` | crude oil price (WTI) | daily |

Open the series page, `https://fred.stlouisfed.org/series/<ID>`, and record its title, frequency, units, and seasonal adjustment. Read the units carefully; you will need them in Q23.

### G2. Add it to the configuration

First, record the current series keys so you can compare them later (Q22). In DuckDB, from the project folder:

```sql
ATTACH 'data/gold/warehouse.duckdb' AS gold (READ_ONLY);
SELECT series_key, series_id FROM gold.dim_series ORDER BY series_key;
```

Save the result, and quit DuckDB (`.quit`). Then add one entry at the end of the `series` list in `dags/config/fred_series.json`, following the pattern of the others (mind the comma after the previous entry):

```json
{
  "id": "<SERIES_ID>",
  "name": "<a short name>",
  "category": "<a category, such as Housing>",
  "frequency": "<daily | weekly | monthly | quarterly>",
  "aggregation_method": "<avg | eop | sum>"
}
```

`frequency` is one of `daily`, `weekly`, `monthly`, `quarterly`, or `yearly`. `aggregation_method` is `avg`, `eop` (end of period), or `sum`; decide it from the units **before** you run anything. Change nothing else.

### G3. Run it and collect evidence in Airflow

1. Trigger **fred_ingest** with `force_all` switched **off**.
2. Open the `select_series` log and find the line beginning `Selected`. Copy it.
3. In the Grid view, find the `fetch_series` instance for your series (the mapped tasks are listed by index; the log names the series). Copy the line beginning `Landed`.
4. Open the `build_silver` log and copy the `Fetched this run` and `Silver row counts` lines.
5. Confirm that **fred_warehouse** started on its own, and copy the four `Check ... passed` lines from its `run_checks` log. Confirm that **fred_reports** followed.

### G4. Verify with DuckDB, layer by layer

From the project folder, start `duckdb` and run these queries, replacing `HOUST` with your series (in the last query, the column is named after your series ID):

```sql
-- Bronze: the raw files and their lineage
SELECT series_id, ingest_date, dag_run_id, try_number, observation_count
FROM read_json('data/bronze/fred/series_id=HOUST/ingest_date=*/_ingest.json');

-- Silver: the catalog entry and the observations
SELECT series_id, frequency, aggregation_method, units, fred_frequency
FROM 'data/silver/fred/series.parquet' WHERE series_id = 'HOUST';

SELECT count(*) AS observations, min(obs_date), max(obs_date)
FROM 'data/silver/fred/observations.parquet' WHERE series_id = 'HOUST';

-- Gold: the dimension, the quarterly fact, and the wide table
ATTACH 'data/gold/warehouse.duckdb' AS gold (READ_ONLY);
USE gold;

SELECT series_key, series_id FROM dim_series ORDER BY series_key;

SELECT q.quarter_label, round(f.value, 1) AS value,
       f.observation_count, f.is_complete
FROM fact_macro_quarterly f
JOIN dim_series  s USING (series_key)
JOIN dim_quarter q USING (quarter_key)
WHERE s.series_id = 'HOUST'
ORDER BY f.quarter_key DESC
LIMIT 4;

SELECT quarter_label, HOUST FROM macro_quarterly_wide
ORDER BY quarter_key DESC LIMIT 4;
```

Also count your series' rows in the native-grain fact table for its frequency (for example `fact_monthly_observation`); the count should match silver.

**Q21.** Your series was fetched even though `force_all` was off. Quote the `Selected` log line, and find the condition in `select_series` (in `dags/fred_ingest_dag.py`) that selected it. Were any other series fetched in the same run? Why or why not?

**Q22.** Compare `dim_series` with the keys you saved in G2. Did the `series_key` of any existing series change? Explain why, using how `series_key` is assigned in `dags/sql/gold/03_dim_series.sql`, and say why that would matter to anyone who saved those keys outside the warehouse.

**Q23.** Justify your `aggregation_method` from the units on the FRED page. What would the latest quarter's value have been with a different method? Which of the four checks would have caught a wrong aggregation method, and which would have caught a wrong frequency?

**Q24.** List every file you changed to add the series. Name each layer and table it reached. What does this tell you about the design of the pipeline?

### G5. Clean up

Unless your instructor asks you to keep the series:

1. Remove your entry from `fred_series.json`.
2. Trigger **fred_ingest** with `force_all` switched **on**. Silver and gold are rebuilt only when something is fetched, so a normal run would leave your series in the warehouse.
3. Optionally, delete `data/bronze/fred/series_id=<ID>/`. Silver ignores bronze folders for series that are not configured, so this is tidy-up, not a requirement.

## Reflection and submission

Answer each reflection question in three to five sentences, citing something you saw in the lab.

1. Bronze stores the raw JSON instead of a cleaned CSV. Name two things you could not do if bronze held only cleaned data.
2. `fred_warehouse` is triggered by an asset instead of a daily clock. What could go wrong with a clock schedule, for example "every day at 02:00"?
3. DuckDB allows only one writer per database file. What would break if each `fetch_series` task wrote straight into `warehouse.duckdb`, and how does the pipeline avoid that?
4. You are asked to add the series `PCE` (personal consumption expenditures, monthly). What do you change, and how do you choose its `aggregation_method`?

Submit:

- [ ] Answers to Q1 to Q20
- [ ] Answers to reflection questions 1 to 4
- [ ] A screenshot of the Grid view from D1 showing a `fetch_series` instance with Try Number 2 or more
- [ ] Your Q17 SQL and its result
- [ ] Confirmation that `fred_config.json` and `fred_series.json` are back to their original settings and your last run is green
- [ ] If Part G was assigned: your configuration entry, the log lines from G3, your G4 queries with their output, and answers to Q21 to Q24

## Troubleshooting and cleanup

Most problems come from Docker resources, the `.env` file, or a SQL client holding the warehouse open.

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `docker compose up` stops with a `FRED_API_KEY` error | no `.env` file, or the key is blank | create `.env` from `.env.example` and add your key |
| `fetch_series` fails at once with HTTP 400 or 403 | wrong API key | fix `.env`, then run `docker compose up -d` to recreate the containers |
| Port 8080 is already in use | another program uses it | stop that program, or change `"8080:8080"` to `"8081:8080"` in `docker-compose.yaml` and use port 8081 |
| Containers keep restarting | Docker has less than 4 GB of memory | raise the memory limit in Docker Desktop settings |
| A DAG is missing from the UI | a Python error in a DAG file | run `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| A run fetches nothing | no frequency is due today | trigger with `force_all` switched on |
| `publish` fails, or DuckDB says it could not set a lock | a client has `warehouse.duckdb` open for writing | close it; always open the warehouse with `-readonly` |
| `fred_warehouse` never starts | it is paused | unpause it; it runs on the next silver update |

When you are done:

1. Stop Airflow: `docker compose down`. Your data in `data/` stays.
2. To start over completely, run `docker compose down -v` (this also deletes Airflow's run history) and delete the `fred` folders under `data/bronze` and `data/silver`, and the files under `data/gold`.
