WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'WAREHOUSE') }}
)
SELECT
    CAST(w_warehouse_sk AS NUMBER) AS warehouse_id,
    CAST(w_warehouse_name AS VARCHAR) AS warehouse_name,
    CAST(w_warehouse_sq_ft AS NUMBER) AS square_feet,
    CAST(w_city AS VARCHAR) AS city,
    CAST(w_state AS VARCHAR) AS state,
    CAST(w_country AS VARCHAR) AS country
    
FROM source