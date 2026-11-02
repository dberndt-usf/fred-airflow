-- GOLD: one fact table per native frequency (the grain of each table is
-- "one series, one period").  Mixing grains in a single fact table
-- invites mistakes, such as averaging daily and monthly values together.
--
--   fact_daily_observation      -> dim_date     (by date)
--   fact_weekly_observation     -> dim_date     (by week-ending date)
--   fact_monthly_observation    -> dim_month    (by YYYYMM)
--   fact_quarterly_observation  -> dim_quarter  (by YYYYQ)
--   fact_annual_observation     -> dim_year     (by YYYY)
--
-- Missing values ('.' in FRED, such as market holidays) are not loaded:
-- a fact row records something that was actually measured.

CREATE TEMP VIEW silver_obs AS
SELECT o.*, s.series_key
FROM '{{ silver_root }}/fred/observations.parquet' o
JOIN dim_series s USING (series_id)
WHERE NOT o.is_missing;


CREATE TABLE fact_daily_observation (
    date_key   INTEGER NOT NULL REFERENCES dim_date (date_key),
    series_key INTEGER NOT NULL REFERENCES dim_series (series_key),
    value      DOUBLE  NOT NULL,
    PRIMARY KEY (date_key, series_key)
);

INSERT INTO fact_daily_observation
SELECT obs_date_key, series_key, value
FROM silver_obs
WHERE frequency_code = 'D'
ORDER BY obs_date_key, series_key;


CREATE TABLE fact_weekly_observation (
    week_ending_date_key INTEGER NOT NULL REFERENCES dim_date (date_key),
    week_key             INTEGER NOT NULL,   -- ISO year + week, for convenience
    series_key           INTEGER NOT NULL REFERENCES dim_series (series_key),
    value                DOUBLE  NOT NULL,
    PRIMARY KEY (week_ending_date_key, series_key)
);

INSERT INTO fact_weekly_observation
SELECT obs_date_key, week_key, series_key, value
FROM silver_obs
WHERE frequency_code = 'W'
ORDER BY obs_date_key, series_key;


CREATE TABLE fact_monthly_observation (
    month_key  INTEGER NOT NULL REFERENCES dim_month (month_key),
    series_key INTEGER NOT NULL REFERENCES dim_series (series_key),
    value      DOUBLE  NOT NULL,
    PRIMARY KEY (month_key, series_key)
);

INSERT INTO fact_monthly_observation
SELECT month_key, series_key, value
FROM silver_obs
WHERE frequency_code = 'M'
ORDER BY month_key, series_key;


CREATE TABLE fact_quarterly_observation (
    quarter_key INTEGER NOT NULL REFERENCES dim_quarter (quarter_key),
    series_key  INTEGER NOT NULL REFERENCES dim_series (series_key),
    value       DOUBLE  NOT NULL,
    PRIMARY KEY (quarter_key, series_key)
);

INSERT INTO fact_quarterly_observation
SELECT quarter_key, series_key, value
FROM silver_obs
WHERE frequency_code = 'Q'
ORDER BY quarter_key, series_key;


CREATE TABLE fact_annual_observation (
    year_key   INTEGER NOT NULL REFERENCES dim_year (year_key),
    series_key INTEGER NOT NULL REFERENCES dim_series (series_key),
    value      DOUBLE  NOT NULL,
    PRIMARY KEY (year_key, series_key)
);

INSERT INTO fact_annual_observation
SELECT year_key, series_key, value
FROM silver_obs
WHERE frequency_code = 'A'
ORDER BY year_key, series_key;
