WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'WEB_SALES') }}
)
SELECT
    --claves
    CAST(ws_order_number AS NUMBER) AS order_number,
    CAST(ws_item_sk AS NUMBER) AS item_id,
    CAST(ws_bill_customer_sk AS NUMBER) AS bill_customer_id,
    CAST(ws_ship_customer_sk AS NUMBER) AS ship_customer_id,
    CAST(ws_web_site_sk AS NUMBER) AS web_site_id,
    CAST(ws_warehouse_sk AS NUMBER) AS warehouse_id,
    CAST(ws_promo_sk AS NUMBER) AS promo_id,
    CAST(ws_sold_date_sk AS NUMBER) AS sold_date_id,
    
    --metricas
    COALESCE(CAST(ws_quantity AS NUMBER), 0) AS quantity,
    COALESCE(CAST(ws_sales_price AS FLOAT), 0.0) AS sales_price,
    COALESCE(CAST(ws_ext_discount_amt AS FLOAT), 0.0) AS discount_amount,
    COALESCE(CAST(ws_ext_sales_price AS FLOAT), 0.0) AS extended_sales_price,
    COALESCE(CAST(ws_ext_ship_cost AS FLOAT), 0.0) AS ship_cost,
    COALESCE(CAST(ws_net_paid AS FLOAT), 0.0) AS net_paid,
    COALESCE(CAST(ws_net_profit AS FLOAT), 0.0) AS net_profit
    
FROM source