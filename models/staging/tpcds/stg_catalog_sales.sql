WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'CATALOG_SALES') }}
)
SELECT
    --claves
    CAST(cs_order_number AS NUMBER) AS order_number,
    CAST(cs_item_sk AS NUMBER) AS item_id,
    CAST(cs_bill_customer_sk AS NUMBER) AS bill_customer_id,
    CAST(cs_ship_customer_sk AS NUMBER) AS ship_customer_id,
    CAST(cs_call_center_sk AS NUMBER) AS call_center_id,
    CAST(cs_warehouse_sk AS NUMBER) AS warehouse_id,
    CAST(cs_promo_sk AS NUMBER) AS promo_id,
    CAST(cs_sold_date_sk AS NUMBER) AS sold_date_id,
    
    --metricas
    COALESCE(CAST(cs_quantity AS NUMBER), 0) AS quantity,
    COALESCE(CAST(cs_sales_price AS FLOAT), 0.0) AS sales_price,
    COALESCE(CAST(cs_ext_discount_amt AS FLOAT), 0.0) AS discount_amount,
    COALESCE(CAST(cs_ext_sales_price AS FLOAT), 0.0) AS extended_sales_price,
    COALESCE(CAST(cs_ext_ship_cost AS FLOAT), 0.0) AS ship_cost,
    COALESCE(CAST(cs_net_paid AS FLOAT), 0.0) AS net_paid,
    COALESCE(CAST(cs_net_profit AS FLOAT), 0.0) AS net_profit

    
FROM source