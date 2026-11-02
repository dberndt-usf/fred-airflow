-- REPORTS: complete quarters since 2000, one column per series, as a CSV
-- file for spreadsheets.  Reads the published gold warehouse (read-only).

COPY (
    SELECT *
    FROM macro_quarterly_wide
    WHERE quarter_key >= 20001
    ORDER BY quarter_key
) TO '{{ reports_root }}/macro_quarterly.csv' (HEADER, DELIMITER ',');
