WITH store_returns AS (
    SELECT 
        'Store' AS return_channel,
        ticket_number AS order_id,
        item_id,
        customer_id,
        store_id AS location_id,
        returned_date_id,
        return_quantity,
        return_amount,
        net_loss
    FROM {{ ref('stg_store_returns') }}
),

catalog_returns AS (
    SELECT 
        'Catalog' AS return_channel,
        order_number AS order_id,
        item_id,
        customer_id,
        call_center_id AS location_id,
        returned_date_id,
        return_quantity,
        return_amount,
        net_loss
    FROM {{ ref('stg_catalog_returns') }}
),

web_returns AS (
    SELECT 
        'Web' AS return_channel,
        order_number AS order_id,
        item_id,
        customer_id,
        web_page_id AS location_id,
        returned_date_id,
        return_quantity,
        return_amount,
        net_loss
    FROM {{ ref('stg_web_returns') }}
)

SELECT * FROM store_returns
UNION ALL
SELECT * FROM catalog_returns
UNION ALL
SELECT * FROM web_returns