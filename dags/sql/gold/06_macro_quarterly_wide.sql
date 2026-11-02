-- GOLD: a wide, analyst-friendly table with one row per quarter and one
-- column per series, built by pivoting fact_macro_quarterly.  Only
-- complete quarters are shown.  This is a convenience table for
-- reporting and charts; the star schema remains the source of truth.

CREATE TABLE macro_quarterly_wide AS
SELECT *
FROM (
    PIVOT (
        SELECT q.quarter_key, q.quarter_label, s.series_id, f.value
        FROM fact_macro_quarterly f
        JOIN dim_quarter q USING (quarter_key)
        JOIN dim_series  s USING (series_key)
        WHERE f.is_complete
    )
    ON series_id
    USING first(value)
)
ORDER BY quarter_key;
