-- Example queries against the gold layer.
--
-- Open the warehouse READ-ONLY so Airflow can still publish new builds:
--   duckdb -readonly data/gold/warehouse.duckdb
-- or in Python:
--   duckdb.connect("data/gold/warehouse.duckdb", read_only=True)
--
-- Silver and bronze can be queried directly from their files, e.g.
--   SELECT * FROM 'data/silver/fred/observations.parquet' LIMIT 10;


-- 1. What is in the warehouse?  (series, frequency, how to roll up)
SELECT series_id, series_name, frequency, aggregation_method, units_short,
       observation_start, observation_end
FROM dim_series
ORDER BY frequency_code, series_id;


-- 2. Star join at native grain: monthly unemployment by year.
SELECT m.year_key, round(avg(f.value), 2) AS avg_unemployment_rate
FROM fact_monthly_observation f
JOIN dim_month  m USING (month_key)
JOIN dim_series s USING (series_key)
WHERE s.series_id = 'UNRATE' AND m.year_key >= 2015
GROUP BY m.year_key
ORDER BY m.year_key;


-- 3. Daily facts by day of week: are Mondays different for the VIX?
SELECT d.day_name, round(avg(f.value), 2) AS avg_vix, count(*) AS trading_days
FROM fact_daily_observation f
JOIN dim_date   d USING (date_key)
JOIN dim_series s USING (series_key)
WHERE s.series_id = 'VIXCLS'
GROUP BY d.day_of_week, d.day_name
ORDER BY d.day_of_week;


-- 4. Drill across: compare series of different frequencies on a common
--    quarterly grain (YYYYQ) through fact_macro_quarterly.
SELECT q.quarter_label,
       max(f.value) FILTER (WHERE s.series_id = 'GDPC1')        AS real_gdp,
       max(f.value) FILTER (WHERE s.series_id = 'UNRATE')       AS unemployment,
       max(f.value) FILTER (WHERE s.series_id = 'DFF')          AS fed_funds,
       max(f.value) FILTER (WHERE s.series_id = 'MORTGAGE30US') AS mortgage_30y,
       max(f.value) FILTER (WHERE s.series_id = 'SP500')        AS sp500_close
FROM fact_macro_quarterly f
JOIN dim_quarter q USING (quarter_key)
JOIN dim_series  s USING (series_key)
WHERE f.is_complete AND q.year_key >= 2022
GROUP BY q.quarter_key, q.quarter_label
ORDER BY q.quarter_key;


-- 5. Growth rates with window functions on the wide table:
--    real GDP growth (annualized) and S&P 500 quarterly return.
SELECT quarter_label,
       round(100 * (power(GDPC1 / lag(GDPC1) OVER w, 4) - 1), 1) AS real_gdp_growth_pct_annualized,
       round(100 * (SP500 / lag(SP500) OVER w - 1), 1)            AS sp500_return_pct
FROM macro_quarterly_wide
WINDOW w AS (ORDER BY quarter_key)
QUALIFY quarter_key >= 20230
ORDER BY quarter_key;


-- 6. Why aggregation_method matters: three ways to roll up M2.
--    M2 is a stock (a level), so end-of-period (eop) is the default.
SELECT o.quarter_key,
       round(avg(o.value), 1)                 AS m2_average,
       round(arg_max(o.value, o.obs_date), 1) AS m2_end_of_period,
       round(sum(o.value), 1)                 AS m2_sum_meaningless
FROM 'data/silver/fred/observations.parquet' o
WHERE o.series_id = 'WM2NS' AND o.quarter_key BETWEEN 20251 AND 20254
GROUP BY o.quarter_key
ORDER BY o.quarter_key;
