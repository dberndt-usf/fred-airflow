-- GOLD: every daily, weekly, monthly, and quarterly series rolled up to a
-- common quarterly grain (YYYYQ), so different series can be compared
-- side by side through the shared dim_quarter ("drill across").
--
-- How a series rolls up depends on what it measures
-- (dim_series.aggregation_method):
--   avg  rates and indexes (unemployment rate, CPI, interest rates)
--   eop  end of period: stock levels (money supply, federal debt, S&P 500)
--   sum  flows that accumulate (monthly retail sales -> quarterly sales)
--
-- Annual series cannot be split into quarters, so they are excluded.
--
-- is_complete is false for the current, still-open quarter (and any
-- quarter after a series' last observation), so a partial quarter is
-- never mistaken for a full one.

CREATE TABLE fact_macro_quarterly (
    quarter_key        INTEGER  NOT NULL REFERENCES dim_quarter (quarter_key),
    series_key         INTEGER  NOT NULL REFERENCES dim_series (series_key),
    value              DOUBLE   NOT NULL,
    aggregation_method VARCHAR  NOT NULL,
    source_frequency   VARCHAR  NOT NULL,   -- D, W, M, Q
    observation_count  SMALLINT NOT NULL,
    is_complete        BOOLEAN  NOT NULL,
    PRIMARY KEY (quarter_key, series_key)
);

INSERT INTO fact_macro_quarterly
WITH coverage AS (
    -- Last day each series has data for.
    SELECT series_key, max(period_end_date) AS covered_through
    FROM silver_obs
    GROUP BY series_key
)
SELECT
    o.quarter_key,
    o.series_key,
    CASE s.aggregation_method
        WHEN 'avg' THEN avg(o.value)
        WHEN 'eop' THEN arg_max(o.value, o.obs_date)   -- value on the latest date
        WHEN 'sum' THEN sum(o.value)
    END                                 AS value,
    s.aggregation_method,
    s.frequency_code                    AS source_frequency,
    count(*)                            AS observation_count,
    q.last_date <= c.covered_through    AS is_complete
FROM silver_obs o
JOIN dim_series  s USING (series_key)
JOIN dim_quarter q USING (quarter_key)
JOIN coverage    c USING (series_key)
WHERE o.frequency_code IN ('D', 'W', 'M', 'Q')
GROUP BY o.quarter_key, o.series_key, s.aggregation_method,
         s.frequency_code, q.last_date, c.covered_through
ORDER BY o.quarter_key, o.series_key;
