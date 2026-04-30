WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'DATE_DIM') }}
)
SELECT
    -- Claves
    CAST(d_date_sk AS NUMBER) AS date_id,
    CAST(d_date_id AS VARCHAR) AS date_alternate_id,
    CAST(d_date AS DATE) AS full_date,
    
    -- Secuencias y números
    CAST(d_month_seq AS NUMBER) AS month_sequence,
    CAST(d_week_seq AS NUMBER) AS week_sequence,
    CAST(d_quarter_seq AS NUMBER) AS quarter_sequence,
    CAST(d_year AS NUMBER) AS year,
    CAST(d_dow AS NUMBER) AS day_of_week,
    CAST(d_moy AS NUMBER) AS month_of_year,
    CAST(d_dom AS NUMBER) AS day_of_month,
    CAST(d_qoy AS NUMBER) AS quarter_of_year,
    
    -- Nombres descriptivos
    CAST(d_day_name AS VARCHAR) AS day_name,
    
    -- Transformando 'Y'/'N' a BOOLEAN real
    CASE WHEN d_holiday = 'Y' THEN TRUE ELSE FALSE END AS is_holiday,
    CASE WHEN d_weekend = 'Y' THEN TRUE ELSE FALSE END AS is_weekend

FROM source