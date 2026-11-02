-- CHECK: every configured series was fetched and has at least one fact.
-- Returns one row per offending series (no rows = pass).
SELECT s.series_id, s.frequency_code
FROM dim_series s
WHERE s.fred_title IS NULL                   -- metadata never fetched
   OR NOT EXISTS (
        SELECT 1 FROM fact_daily_observation     f WHERE f.series_key = s.series_key UNION ALL
        SELECT 1 FROM fact_weekly_observation    f WHERE f.series_key = s.series_key UNION ALL
        SELECT 1 FROM fact_monthly_observation   f WHERE f.series_key = s.series_key UNION ALL
        SELECT 1 FROM fact_quarterly_observation f WHERE f.series_key = s.series_key UNION ALL
        SELECT 1 FROM fact_annual_observation    f WHERE f.series_key = s.series_key
   );
