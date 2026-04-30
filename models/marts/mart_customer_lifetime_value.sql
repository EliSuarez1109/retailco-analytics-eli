{{ config(materialized='table') }}

WITH customer_sales AS (
    SELECT 
        s.customer_id,
        COUNT(DISTINCT s.order_id) AS total_transactions,
        COUNT(DISTINCT IFF(s.sales_channel = 'Store', s.order_id, NULL)) AS store_transactions,
        COUNT(DISTINCT IFF(s.sales_channel = 'Web', s.order_id, NULL)) AS web_transactions,
        COUNT(DISTINCT IFF(s.sales_channel = 'Catalog', s.order_id, NULL)) AS catalog_transactions,
        MIN(d.full_date) AS first_purchase_date,
        MAX(d.full_date) AS last_purchase_date,
        SUM(s.net_paid) AS total_clv,
        SUM(s.sales_price * s.quantity) AS gross_revenue
    FROM {{ ref('int_unified_sales') }} s
    JOIN {{ ref('stg_date_dim') }} d ON s.sold_date_id = d.date_id
    WHERE s.customer_id IS NOT NULL
    GROUP BY 1
),

customer_returns AS (
    SELECT 
        r.customer_id,
        SUM(r.return_amount) AS total_returned_amount
    FROM {{ ref('int_unified_returns') }} r
    WHERE r.customer_id IS NOT NULL
    GROUP BY 1
)

SELECT 
    s.customer_id,   
    e.full_name,
    e.email_address,
    e.age_group,
    s.total_transactions,
    s.store_transactions,
    s.web_transactions,
    s.catalog_transactions,
    s.first_purchase_date,
    s.last_purchase_date,
    s.total_clv,
    COALESCE(r.total_returned_amount, 0) AS total_returned_amount,
    
    -- Tasa de devolución
    IFF(s.gross_revenue > 0, (COALESCE(r.total_returned_amount, 0) / s.gross_revenue) * 100, 0) AS return_rate_pct,
    
    -- Segmentación basada en tus datos reales
    CASE 
        WHEN s.total_clv >= 309725.43 THEN 'High'
        WHEN s.total_clv >= 238633.76 THEN 'Medium'
        ELSE 'Low'
    END AS value_segment

FROM customer_sales s
LEFT JOIN customer_returns r ON s.customer_id = r.customer_id
LEFT JOIN {{ ref('int_customer_enriched') }} e ON s.customer_id = e.customer_id