-- CHECK: dim_date has exactly one row for every day in its range.
SELECT min(full_date) AS first_day, max(full_date) AS last_day,
       count(*) AS row_count,
       max(full_date) - min(full_date) + 1 AS expected_rows
FROM dim_date
HAVING count(*) <> max(full_date) - min(full_date) + 1;
