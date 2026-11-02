-- GOLD: the series dimension (one row per FRED series).
-- series_key is a surrogate key.  Because the warehouse is rebuilt from
-- silver on every run, keys are assigned in series_id order and can
-- shift when series are added (see the README exercises).

CREATE TABLE dim_series (
    series_key          INTEGER PRIMARY KEY,
    series_id           VARCHAR NOT NULL UNIQUE,   -- FRED's natural key
    series_name         VARCHAR NOT NULL,
    fred_title          VARCHAR,
    category            VARCHAR NOT NULL,
    frequency           VARCHAR NOT NULL,          -- daily, weekly, ...
    frequency_code      VARCHAR NOT NULL,          -- D, W, M, Q, A
    aggregation_method  VARCHAR NOT NULL,          -- avg, eop, sum
    units               VARCHAR,
    units_short         VARCHAR,
    seasonal_adjustment VARCHAR,
    observation_start   DATE,
    observation_end     DATE,
    fred_last_updated   TIMESTAMPTZ,
    CHECK (aggregation_method IN ('avg', 'eop', 'sum')),
    CHECK (frequency_code IN ('D', 'W', 'M', 'Q', 'A'))
);

INSERT INTO dim_series
SELECT
    row_number() OVER (ORDER BY series_id) AS series_key,
    series_id,
    series_name,
    fred_title,
    category,
    frequency,
    frequency_code,
    aggregation_method,
    units,
    units_short,
    seasonal_adjustment,
    observation_start,
    observation_end,
    fred_last_updated
FROM '{{ silver_root }}/fred/series.parquet'
ORDER BY series_id;
