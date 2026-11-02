-- GOLD: the date dimension (one row per calendar day).
-- Generated entirely in SQL; this replaces the old Oracle stored
-- procedure UPDATE_DATE_DIMENSION.  It spans January 1 of the earliest
-- observation year through December 31 of the latest year.

CREATE TABLE dim_date (
    date_key        INTEGER  PRIMARY KEY,   -- YYYYMMDD, e.g. 20260701
    full_date       DATE     NOT NULL UNIQUE,
    day_of_month    TINYINT  NOT NULL,
    day_of_week     TINYINT  NOT NULL,      -- ISO: 1 = Monday ... 7 = Sunday
    day_name        VARCHAR  NOT NULL,
    is_weekend      BOOLEAN  NOT NULL,
    iso_year        SMALLINT NOT NULL,
    iso_week        TINYINT  NOT NULL,
    week_key        INTEGER  NOT NULL,      -- ISO year + week, e.g. 202627
    month_key       INTEGER  NOT NULL,      -- YYYYMM, e.g. 202607
    month_num       TINYINT  NOT NULL,
    month_name      VARCHAR  NOT NULL,
    quarter_key     INTEGER  NOT NULL,      -- YYYYQ, e.g. 20263
    quarter_num     TINYINT  NOT NULL,
    quarter_label   VARCHAR  NOT NULL,      -- e.g. '2026-Q3'
    year_key        INTEGER  NOT NULL,      -- YYYY
    is_month_end    BOOLEAN  NOT NULL,
    is_quarter_end  BOOLEAN  NOT NULL,
    is_year_end     BOOLEAN  NOT NULL
);

INSERT INTO dim_date
WITH bounds AS (
    SELECT
        make_date(year(min(period_start_date)), 1, 1)                        AS first_day,
        make_date(greatest(year(max(period_end_date)), year(current_date)), 12, 31) AS last_day
    FROM '{{ silver_root }}/fred/observations.parquet'
),
days AS (
    SELECT CAST(unnest(range(CAST(first_day AS TIMESTAMP),
                             CAST(last_day AS TIMESTAMP) + INTERVAL 1 DAY,
                             INTERVAL 1 DAY)) AS DATE) AS d
    FROM bounds
)
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER)       AS date_key,
    d                                            AS full_date,
    day(d)                                       AS day_of_month,
    isodow(d)                                    AS day_of_week,
    dayname(d)                                   AS day_name,
    isodow(d) IN (6, 7)                          AS is_weekend,
    isoyear(d)                                   AS iso_year,
    week(d)                                      AS iso_week,
    isoyear(d) * 100 + week(d)                   AS week_key,
    year(d) * 100 + month(d)                     AS month_key,
    month(d)                                     AS month_num,
    monthname(d)                                 AS month_name,
    year(d) * 10 + quarter(d)                    AS quarter_key,
    quarter(d)                                   AS quarter_num,
    year(d) || '-Q' || quarter(d)                AS quarter_label,
    year(d)                                      AS year_key,
    d = last_day(d)                              AS is_month_end,
    d = last_day(d) AND month(d) IN (3, 6, 9, 12) AS is_quarter_end,
    month(d) = 12 AND day(d) = 31                AS is_year_end
FROM days
ORDER BY d;
