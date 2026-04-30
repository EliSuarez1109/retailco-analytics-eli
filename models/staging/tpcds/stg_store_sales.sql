WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'STORE_SALES') }}
)
SELECT
    -- Claves
    CAST(ss_ticket_number AS NUMBER) AS ticket_number,
    CAST(ss_item_sk AS NUMBER) AS item_id,
    CAST(ss_customer_sk AS NUMBER) AS customer_id,
    CAST(ss_store_sk AS NUMBER) AS store_id,
    CAST(ss_promo_sk AS NUMBER) AS promo_id,
    CAST(ss_sold_date_sk AS NUMBER) AS sold_date_id,
    CAST(ss_sold_time_sk AS NUMBER) AS sold_time_id,
    
    -- Métricas
    COALESCE(CAST(ss_quantity AS NUMBER), 0) AS quantity,
    COALESCE(CAST(ss_wholesale_cost AS FLOAT), 0.0) AS wholesale_cost,
    COALESCE(CAST(ss_list_price AS FLOAT), 0.0) AS list_price,
    COALESCE(CAST(ss_sales_price AS FLOAT), 0.0) AS sales_price,
    COALESCE(CAST(ss_ext_discount_amt AS FLOAT), 0.0) AS discount_amount,
    COALESCE(CAST(ss_ext_sales_price AS FLOAT), 0.0) AS extended_sales_price,
    COALESCE(CAST(ss_net_paid AS FLOAT), 0.0) AS net_paid,
    COALESCE(CAST(ss_net_profit AS FLOAT), 0.0) AS net_profit

FROM source