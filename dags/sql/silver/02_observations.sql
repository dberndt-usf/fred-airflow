-- SILVER: one row per series per observation date.
--
-- Steps:
--   1. Read every bronze observations.json (schema-on-read: we declare
--      the JSON structure we expect, and DuckDB enforces the types).
--   2. Unnest the observations array into rows.
--   3. Deduplicate: each fetch lands the FULL history again, so keep
--      only the latest ingest for each (series, date).  FRED revises
--      past values (e.g., GDP), so the latest ingest wins.
--   4. Clean: FRED uses '.' for missing values.
--   5. Add the period each observation covers and its calendar keys.

COPY (
    WITH bronze AS (
        SELECT
            series_id,
            CAST(ingest_date AS DATE) AS ingest_date,
            unnest(observations)      AS o
        FROM read_json(
            '{{ bronze_root }}/fred/series_id=*/ingest_date=*/observations.json',
            hive_partitioning = true,
            maximum_object_size = 268435456,
            columns = {
                'units': 'VARCHAR',
                'count': 'INTEGER',
                'observations': 'STRUCT(realtime_start DATE, realtime_end DATE, "date" DATE, "value" VARCHAR)[]'
            }
        )
    ),
    latest AS (
        SELECT *
        FROM bronze
        QUALIFY row_number() OVER (
            PARTITION BY series_id, o.date
            ORDER BY ingest_date DESC
        ) = 1
    ),
    typed AS (
        SELECT
            l.series_id,
            s.frequency_code,
            l.o.date                                    AS obs_date,
            TRY_CAST(NULLIF(l.o.value, '.') AS DOUBLE)  AS value,
            l.o.realtime_start                          AS realtime_start,
            l.ingest_date,
            -- FRED dates a period by its FIRST day, except weekly series,
            -- which are dated by the LAST day of the week (e.g., M2 is
            -- "Weekly, Ending Monday").  Make the covered period explicit.
            CASE s.frequency_code
                WHEN 'W' THEN l.o.date - 6
                ELSE l.o.date
            END                                         AS period_start_date,
            CASE s.frequency_code
                WHEN 'D' THEN l.o.date
                WHEN 'W' THEN l.o.date
                WHEN 'M' THEN last_day(l.o.date)
                WHEN 'Q' THEN CAST(l.o.date + INTERVAL 3 MONTH - INTERVAL 1 DAY AS DATE)
                WHEN 'A' THEN make_date(year(l.o.date), 12, 31)
            END                                         AS period_end_date
        FROM latest l
        JOIN '{{ silver_root }}/fred/series.parquet' s USING (series_id)
    )
    SELECT
        series_id,
        frequency_code,
        obs_date,
        value,
        value IS NULL                                     AS is_missing,
        period_start_date,
        period_end_date,
        -- Calendar keys.  Integer "smart keys" are readable, sort
        -- correctly, and join cheaply:  2026-07-01 -> 20260701,
        -- ISO week 202627, month 202607, quarter 20263, year 2026.
        -- Rollup keys come from the END of the period, so a week that
        -- starts in March but ends in April belongs to April / Q2.
        CAST(strftime(obs_date, '%Y%m%d') AS INTEGER)     AS obs_date_key,
        CAST(isoyear(period_end_date) * 100 + week(period_end_date) AS INTEGER) AS week_key,
        CAST(year(period_end_date) * 100 + month(period_end_date) AS INTEGER)   AS month_key,
        CAST(year(period_end_date) * 10 + quarter(period_end_date) AS INTEGER)  AS quarter_key,
        CAST(year(period_end_date) AS INTEGER)            AS year_key,
        realtime_start,
        ingest_date
    FROM typed
    ORDER BY series_id, obs_date
) TO '{{ silver_root }}/fred/observations.parquet' (FORMAT parquet);
