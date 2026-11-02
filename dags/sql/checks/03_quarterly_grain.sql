-- CHECK: no quarter holds more observations than its source frequency
-- allows.  Too many rows usually means duplicates slipped through silver.
SELECT q.quarter_label, s.series_id, f.source_frequency, f.observation_count
FROM fact_macro_quarterly f
JOIN dim_quarter q USING (quarter_key)
JOIN dim_series  s USING (series_key)
WHERE f.observation_count > CASE f.source_frequency
                                WHEN 'D' THEN 92
                                WHEN 'W' THEN 14
                                WHEN 'M' THEN 3
                                WHEN 'Q' THEN 1
                            END;
