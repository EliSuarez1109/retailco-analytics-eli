WITH store_sales AS (
    SELECT 
        'Store' AS sales_channel,
        ticket_number AS order_id,
        item_id,
        customer_id,
        store_id AS location_id,
        NULL AS warehouse_id, 
        promo_id,
        sold_date_id,
        quantity,
        sales_price,
        discount_amount,
        extended_sales_price,
        net_paid,
        net_profit
    FROM {{ ref('stg_store_sales') }}
),

catalog_sales AS (
    SELECT 
        'Catalog' AS sales_channel,
        order_number AS order_id,
        item_id,
        bill_customer_id AS customer_id, 
        call_center_id AS location_id,
        warehouse_id,
        promo_id,
        sold_date_id,
        quantity,
        sales_price,
        discount_amount,
        extended_sales_price,
        net_paid,
        net_profit
    FROM {{ ref('stg_catalog_sales') }}
),

web_sales AS (
    SELECT 
        'Web' AS sales_channel,
        order_number AS order_id,
        item_id,
        bill_customer_id AS customer_id,
        web_site_id AS location_id,
        warehouse_id,
        promo_id,
        sold_date_id,
        quantity,
        sales_price,
        discount_amount,
        extended_sales_price,
        net_paid,
        net_profit
    FROM {{ ref('stg_web_sales') }}
)

SELECT * FROM store_sales
UNION ALL
SELECT * FROM catalog_sales
UNION ALL
SELECT * FROM web_sales