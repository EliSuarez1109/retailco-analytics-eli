WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'PROMOTION') }}
)
SELECT
    CAST(p_promo_sk AS NUMBER) AS promo_id,
    CAST(p_promo_id AS VARCHAR) AS promo_alternate_id,
    CAST(p_promo_name AS VARCHAR) AS promo_name,
    COALESCE(CAST(p_cost AS FLOAT), 0.0) AS promo_cost,
    CAST(p_channel_dmail AS VARCHAR) AS channel_dmail,
    CAST(p_channel_email AS VARCHAR) AS channel_email,
    CAST(p_channel_tv AS VARCHAR) AS channel_tv
    
FROM source