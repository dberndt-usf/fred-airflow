-- DuckDB demo: bronze -> silver -> gold in one session.
--
-- Start DuckDB in memory from the project folder:
--     duckdb
-- Then paste one query at a time, or run the whole script:
--     .read analysis/demo_queries.sql
--
-- Bronze and silver are plain files, queried in place.  Gold is attached
-- READ-ONLY so Airflow can still publish new warehouse builds.

.timer on
.mode box
.maxrows 20


-- =====================================================================
-- 1. BRONZE: the raw JSON, exactly as FRED sent it
-- =====================================================================

-- 1a. What has landed?  One row per series per ingest date.
SELECT series_id, ingest_date, count AS observations, units
FROM read_json('data/bronze/fred/series_id=*/ingest_date=*/observations.json',
               hive_partitioning = true,
               columns = {'count': 'INTEGER', 'units': 'VARCHAR'})
ORDER BY observations DESC;

-- 1b. Inside one raw response: the envelope and the first three
--     observations.  Note the values are text, and '.' means "no value".
SELECT count, realtime_start, observations[1:3] AS first_three
FROM read_json('data/bronze/fred/series_id=UNRATE/ingest_date=*/observations.json');

-- 1c. Lineage: which run fetched each file, and on which try.
SELECT series_id, ingest_date, try_number, observation_count, bytes
FROM read_json('data/bronze/fred/series_id=*/ingest_date=*/_ingest.json',
               hive_partitioning = false)
ORDER BY bytes DESC;


-- =====================================================================
-- 2. SILVER: typed, cleaned, deduplicated Parquet
-- =====================================================================

-- 2a. The series catalog: our config merged with FRED's metadata.
SELECT series_id, series_name, frequency, aggregation_method, units_short,
       observation_start, observation_end
FROM 'data/silver/fred/series.parquet'
ORDER BY frequency_code, series_id;

-- 2b. Profile every column of the observations in one command.
SUMMARIZE FROM 'data/silver/fred/observations.parquet';

-- 2c. Missing values since 2025: market holidays and the
--     October 2025 government shutdown.
SELECT series_id, count(*) AS missing_days, min(obs_date), max(obs_date)
FROM 'data/silver/fred/observations.parquet'
WHERE is_missing AND obs_date >= '2025-01-01'
GROUP BY series_id
ORDER BY missing_days DESC;

-- 2d. Calendar keys: weekly series are dated by the END of the week,
--     monthly and quarterly series by the START of the period.
SELECT series_id, obs_date, dayname(obs_date) AS weekday,
       period_start_date, period_end_date, month_key, quarter_key
FROM 'data/silver/fred/observations.parquet'
WHERE (series_id = 'WM2NS' AND obs_date >= '2026-08-01')
   OR (series_id IN ('UNRATE', 'GDP') AND obs_date >= '2026-04-01')
ORDER BY series_id, obs_date;


-- =====================================================================
-- 3. GOLD: star schemas in the warehouse (read-only)
-- =====================================================================

ATTACH 'data/gold/warehouse.duckdb' AS gold (READ_ONLY);
USE gold;
SHOW TABLES;

-- 3a. Star join at native grain: average unemployment by year.
SELECT m.year_key, round(avg(f.value), 2) AS avg_unemployment_rate
FROM fact_monthly_observation f
JOIN dim_month  m USING (month_key)
JOIN dim_series s USING (series_key)
WHERE s.series_id = 'UNRATE' AND m.year_key >= 2015
GROUP BY m.year_key
ORDER BY m.year_key;

-- 3b. Daily facts by day of week: is the VIX higher on Mondays?
SELECT d.day_name, round(avg(f.value), 2) AS avg_vix, count(*) AS trading_days
FROM fact_daily_observation f
JOIN dim_date   d USING (date_key)
JOIN dim_series s USING (series_key)
WHERE s.series_id = 'VIXCLS'
GROUP BY d.day_of_week, d.day_name
ORDER BY d.day_of_week;

-- 3c. Drill across: daily, weekly, monthly, and quarterly series side by
--     side on one quarterly key (YYYYQ).
SELECT q.quarter_label,
       max(f.value) FILTER (WHERE s.series_id = 'GDPC1')        AS real_gdp,
       max(f.value) FILTER (WHERE s.series_id = 'UNRATE')       AS unemployment,
       max(f.value) FILTER (WHERE s.series_id = 'DFF')          AS fed_funds,
       max(f.value) FILTER (WHERE s.series_id = 'MORTGAGE30US') AS mortgage_30y
FROM fact_macro_quarterly f
JOIN dim_quarter q USING (quarter_key)
JOIN dim_series  s USING (series_key)
WHERE f.is_complete AND q.year_key >= 2024
GROUP BY q.quarter_key, q.quarter_label
ORDER BY q.quarter_key;

-- 3d. Quarters since 2000 when real GDP shrank (window function + QUALIFY).
SELECT quarter_label,
       round(100 * (GDPC1 / lag(GDPC1) OVER w - 1), 2) AS real_gdp_change_pct,
       round(UNRATE, 1) AS unemployment
FROM macro_quarterly_wide
WINDOW w AS (ORDER BY quarter_key)
QUALIFY real_gdp_change_pct < 0 AND quarter_key >= 20001
ORDER BY quarter_key;

-- 3e. Do unemployment and the fed funds rate move together?
--     Correlation by decade.
SELECT quarter_key // 100 * 10 AS decade,
       round(corr(UNRATE, DFF), 2) AS corr_unemployment_fed_funds,
       count(*) AS quarters
FROM macro_quarterly_wide
WHERE UNRATE IS NOT NULL AND DFF IS NOT NULL
GROUP BY decade
ORDER BY decade;

-- 3f. Why aggregation_method matters: three ways to roll up M2.
--     M2 is a level, so end-of-period is right and the sum is meaningless.
SELECT o.quarter_key,
       round(avg(o.value), 1)                 AS m2_average,
       round(arg_max(o.value, o.obs_date), 1) AS m2_end_of_period,
       round(sum(o.value), 1)                 AS m2_sum_meaningless
FROM 'data/silver/fred/observations.parquet' o
WHERE o.series_id = 'WM2NS' AND o.quarter_key BETWEEN 20251 AND 20254
GROUP BY o.quarter_key
ORDER BY o.quarter_key;

-- 3g. The open quarter: which series are still incomplete, and why.
SELECT s.series_id, s.frequency, f.observation_count, f.is_complete
FROM fact_macro_quarterly f
JOIN dim_series s USING (series_key)
WHERE f.quarter_key = (SELECT max(quarter_key) FROM fact_macro_quarterly WHERE is_complete)
ORDER BY f.is_complete, s.frequency_code, s.series_id;


-- =====================================================================
-- 4. Optional: export a result for a spreadsheet (writes a file)
-- =====================================================================
-- COPY (FROM macro_quarterly_wide WHERE quarter_key >= 20001)
--   TO 'macro_quarterly_export.csv' (HEADER);
