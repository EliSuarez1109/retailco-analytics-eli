WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'WEB_RETURNS') }}
)
SELECT
    --clave
    CAST(wr_order_number AS NUMBER) AS order_number,
    CAST(wr_item_sk AS NUMBER) AS item_id,
    CAST(wr_returning_customer_sk AS NUMBER) AS customer_id,
    CAST(wr_web_page_sk AS NUMBER) AS web_page_id,
    CAST(wr_returned_date_sk AS NUMBER) AS returned_date_id,

    --metricas
    COALESCE(CAST(wr_return_qty AS NUMBER), 0) AS return_quantity,
    COALESCE(CAST(wr_return_amt AS FLOAT), 0.0) AS return_amount,
    COALESCE(CAST(wr_net_loss AS FLOAT), 0.0) AS net_loss
    
FROM source