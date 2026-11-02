-- GOLD: "shrunken" rollup dimensions for month, quarter, and year.
-- Monthly, quarterly, and yearly facts cannot join to dim_date (there is
-- no single day for "2026-Q3"), so they join to these coarser dimensions
-- instead.  Each is derived from dim_date, so the attributes always agree.

CREATE TABLE dim_month (
    month_key     INTEGER PRIMARY KEY,   -- YYYYMM
    year_key      INTEGER NOT NULL,
    quarter_key   INTEGER NOT NULL,
    month_num     TINYINT NOT NULL,
    month_name    VARCHAR NOT NULL,
    month_label   VARCHAR NOT NULL,      -- e.g. '2026-07'
    first_date    DATE    NOT NULL,
    last_date     DATE    NOT NULL,
    days_in_month TINYINT NOT NULL
);

INSERT INTO dim_month
SELECT
    month_key,
    any_value(year_key),
    any_value(quarter_key),
    any_value(month_num),
    any_value(month_name),
    strftime(min(full_date), '%Y-%m'),
    min(full_date),
    max(full_date),
    count(*)
FROM dim_date
GROUP BY month_key
ORDER BY month_key;


CREATE TABLE dim_quarter (
    quarter_key     INTEGER PRIMARY KEY,  -- YYYYQ
    year_key        INTEGER  NOT NULL,
    quarter_num     TINYINT  NOT NULL,
    quarter_label   VARCHAR  NOT NULL,    -- e.g. '2026-Q3'
    first_date      DATE     NOT NULL,
    last_date       DATE     NOT NULL,
    days_in_quarter SMALLINT NOT NULL
);

INSERT INTO dim_quarter
SELECT
    quarter_key,
    any_value(year_key),
    any_value(quarter_num),
    any_value(quarter_label),
    min(full_date),
    max(full_date),
    count(*)
FROM dim_date
GROUP BY quarter_key
ORDER BY quarter_key;


CREATE TABLE dim_year (
    year_key     INTEGER  PRIMARY KEY,   -- YYYY
    decade       INTEGER  NOT NULL,      -- e.g. 2020
    is_leap_year BOOLEAN  NOT NULL,
    first_date   DATE     NOT NULL,
    last_date    DATE     NOT NULL,
    days_in_year SMALLINT NOT NULL
);

INSERT INTO dim_year
SELECT
    year_key,
    year_key // 10 * 10,
    count(*) = 366,
    min(full_date),
    max(full_date),
    count(*)
FROM dim_date
GROUP BY year_key
ORDER BY year_key;
