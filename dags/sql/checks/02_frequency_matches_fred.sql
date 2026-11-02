-- CHECK: the frequency in config/fred_series.json matches what FRED reports.
-- A mismatch would put a series in the wrong fact table.
SELECT series_id, frequency AS configured, fred_frequency AS fred_says
FROM '{{ silver_root }}/fred/series.parquet'
WHERE fred_frequency_code IS DISTINCT FROM frequency_code;
