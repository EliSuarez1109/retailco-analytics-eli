WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'STORE_RETURNS') }}
)
SELECT
    --claves
    CAST(sr_ticket_number AS NUMBER) AS ticket_number,
    CAST(sr_item_sk AS NUMBER) AS item_id,
    CAST(sr_customer_sk AS NUMBER) AS customer_id,
    CAST(sr_store_sk AS NUMBER) AS store_id,
    CAST(sr_reason_sk AS NUMBER) AS reason_id,
    CAST(sr_returned_date_sk AS NUMBER) AS returned_date_id,
    
    --metricas
    COALESCE(CAST(sr_return_quantity AS NUMBER), 0) AS return_quantity,
    COALESCE(CAST(sr_return_amt AS FLOAT), 0.0) AS return_amount,
    COALESCE(CAST(sr_net_loss AS FLOAT), 0.0) AS net_loss
    
FROM source