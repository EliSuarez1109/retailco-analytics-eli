{{ config(
    materialized='incremental',
    unique_key=['sales_month', 'sales_channel', 'location_id', 'category_name']
) }}

WITH sales AS (
    SELECT 
        s.sales_channel,
        s.location_id,
        i.category_name,
        DATE_TRUNC('month', d.full_date) AS sales_month,
        SUM(s.sales_price * s.quantity) AS gross_revenue,
        SUM(s.discount_amount) AS total_discount,
        SUM(s.quantity) AS units_sold
    FROM {{ ref('int_unified_sales') }} s
    LEFT JOIN {{ ref('stg_date_dim') }} d ON s.sold_date_id = d.date_id
    LEFT JOIN {{ ref('stg_item') }} i ON s.item_id = i.item_id
    {% if is_incremental() %}
        -- Estrategia de datos retroactivos: reprocesar los últimos 2 meses por si hay ventas tardías
        WHERE d.full_date >= DATEADD(month, -2, CURRENT_DATE)
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

returns AS (
    SELECT 
        r.return_channel,
        r.location_id,
        i.category_name,
        DATE_TRUNC('month', d.full_date) AS return_month,
        SUM(r.return_amount) AS total_return_amount
    FROM {{ ref('int_unified_returns') }} r
    LEFT JOIN {{ ref('stg_date_dim') }} d ON r.returned_date_id = d.date_id
    LEFT JOIN {{ ref('stg_item') }} i ON r.item_id = i.item_id
    {% if is_incremental() %}
        WHERE d.full_date >= DATEADD(month, -2, CURRENT_DATE)
    {% endif %}
    GROUP BY 1, 2, 3, 4
)

SELECT 
    s.sales_month,
    s.sales_channel,
    s.location_id,
    s.category_name,
    s.gross_revenue,
    COALESCE(r.total_return_amount, 0) AS total_return_amount,
    s.total_discount,
    
    -- Métricas calculadas
    (s.gross_revenue - COALESCE(r.total_return_amount, 0)) AS net_revenue,
    
    -- Descuento medio en porcentaje
    IFF(s.gross_revenue > 0, (s.total_discount / s.gross_revenue) * 100, 0) AS avg_discount_pct,
    
    -- Tasa de devolución en dinero
    IFF(s.gross_revenue > 0, (COALESCE(r.total_return_amount, 0) / s.gross_revenue) * 100, 0) AS return_rate_pct

FROM sales s
LEFT JOIN returns r 
    ON s.sales_month = r.return_month 
    AND s.sales_channel = r.return_channel 
    AND s.location_id = r.location_id 
    AND s.category_name = r.category_name