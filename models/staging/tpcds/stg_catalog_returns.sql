WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'CATALOG_RETURNS') }}
)
SELECT
    --claves
    CAST(cr_order_number AS NUMBER) AS order_number,
    CAST(cr_item_sk AS NUMBER) AS item_id,
    CAST(cr_returning_customer_sk AS NUMBER) AS customer_id,
    CAST(cr_call_center_sk AS NUMBER) AS call_center_id,
    CAST(cr_returned_date_sk AS NUMBER) AS returned_date_id,
    
    --metricas
    COALESCE(CAST(cr_return_quantity AS NUMBER), 0) AS return_quantity,
    COALESCE(CAST(cr_return_amount AS FLOAT), 0.0) AS return_amount,
    COALESCE(CAST(cr_net_loss AS FLOAT), 0.0) AS net_loss
    
FROM source