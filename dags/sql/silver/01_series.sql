-- SILVER: one row per configured FRED series.
-- Combines our own configuration (name, category, aggregation method)
-- with the metadata FRED returns (title, units, seasonal adjustment).
-- Only the most recent bronze snapshot of the metadata is kept.

COPY (
    WITH config AS (
        SELECT unnest(series, recursive := true)
        FROM read_json('{{ config_dir }}/fred_series.json')
    ),
    fred_meta AS (
        SELECT
            series_id,
            ingest_date,
            unnest(seriess) AS s          -- FRED really does spell it "seriess".
        FROM read_json(
            '{{ bronze_root }}/fred/series_id=*/ingest_date=*/series.json',
            hive_partitioning = true
        )
        QUALIFY row_number() OVER (PARTITION BY series_id ORDER BY ingest_date DESC) = 1
    )
    SELECT
        c.id                                AS series_id,
        c.name                              AS series_name,
        m.s.title                           AS fred_title,
        c.category                          AS category,
        c.frequency                         AS frequency,
        -- Normalize our frequency names to FRED's one-letter codes.
        CASE c.frequency
            WHEN 'daily'     THEN 'D'
            WHEN 'weekly'    THEN 'W'
            WHEN 'monthly'   THEN 'M'
            WHEN 'quarterly' THEN 'Q'
            WHEN 'yearly'    THEN 'A'
        END                                 AS frequency_code,
        m.s.frequency_short                 AS fred_frequency_code,
        m.s.frequency                       AS fred_frequency,
        c.aggregation_method                AS aggregation_method,
        m.s.units                           AS units,
        m.s.units_short                     AS units_short,
        m.s.seasonal_adjustment_short       AS seasonal_adjustment,
        CAST(m.s.observation_start AS DATE) AS observation_start,
        CAST(m.s.observation_end AS DATE)   AS observation_end,
        -- FRED timestamps look like '2026-09-22 12:01:36-05'.
        strptime(m.s.last_updated || '00', '%Y-%m-%d %H:%M:%S%z') AS fred_last_updated,
        CAST(m.ingest_date AS DATE)         AS ingest_date
    FROM config c
    LEFT JOIN fred_meta m ON m.series_id = c.id
    ORDER BY c.id
) TO '{{ silver_root }}/fred/series.parquet' (FORMAT parquet);
