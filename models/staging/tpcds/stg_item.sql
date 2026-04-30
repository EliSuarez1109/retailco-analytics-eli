WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'ITEM') }}
)
SELECT
    CAST(i_item_sk AS NUMBER) AS item_id,
    CAST(i_item_id AS VARCHAR) AS item_alternate_id,
    CAST(i_item_desc AS VARCHAR) AS item_description,
    CAST(i_brand AS VARCHAR) AS brand_name,
    CAST(i_class AS VARCHAR) AS class_name,
    CAST(i_category AS VARCHAR) AS category_name,
    CAST(i_manufact AS VARCHAR) AS manufacturer_name,
    CAST(i_color AS VARCHAR) AS color,
    CAST(i_product_name AS VARCHAR) AS product_name,
    CAST(i_current_price AS FLOAT) AS current_price,
    
    --fecha
    CAST(i_rec_start_date AS DATE) AS valid_from,
    CAST(i_rec_end_date AS DATE) AS valid_to,
    --columna derivada 
    CASE WHEN i_rec_end_date IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM source