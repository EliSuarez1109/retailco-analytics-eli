{{ config(materialized='table') }}

WITH weekly_sales AS (
    SELECT 
        s.item_id,
        d.year,
        d.week_sequence,
        SUM(s.quantity) AS weekly_sales_qty
    FROM {{ ref('int_unified_sales') }} s
    JOIN {{ ref('stg_date_dim') }} d ON s.sold_date_id = d.date_id
    GROUP BY 1, 2, 3
),

weekly_inventory AS (
    SELECT 
        i.inv_item_sk AS item_id,
        d.year,
        d.week_sequence,
        AVG(i.inv_quantity_on_hand) AS avg_weekly_stock,
        SUM(IFF(i.inv_quantity_on_hand < 5, 1, 0)) AS days_out_of_stock
    FROM {{ source('tpcds_raw', 'INVENTORY') }} i
    JOIN {{ ref('stg_date_dim') }} d ON i.inv_date_sk = d.date_id
    GROUP BY 1, 2, 3
),

inventory_metrics AS (
    SELECT 
        i.item_id,
        i.year,
        i.week_sequence,
        i.avg_weekly_stock,
        COALESCE(s.weekly_sales_qty, 0) AS weekly_sales_qty,
        
    
        IFF(COALESCE(s.weekly_sales_qty, 0) > 0, 
            i.avg_weekly_stock / s.weekly_sales_qty, 
            999) AS weeks_of_stock,
            
    
        IFF(i.days_out_of_stock > 0, 1, 0) AS has_stockout
        
    FROM weekly_inventory i
    LEFT JOIN weekly_sales s 
        ON i.item_id = s.item_id 
        AND i.year = s.year 
        AND i.week_sequence = s.week_sequence
)

SELECT 
    item_id,
    year,
    week_sequence,
    avg_weekly_stock,
    weekly_sales_qty,
    weeks_of_stock,
    has_stockout,
    
    LAG(avg_weekly_stock) OVER (PARTITION BY item_id ORDER BY year, week_sequence) AS prev_week_stock,
    
   -- Usamos nuestra macro personalizada para estandarizar la regla de negocio
    {{ clasificacion_tendencia('avg_weekly_stock', 'LAG(avg_weekly_stock) OVER (PARTITION BY item_id ORDER BY year, week_sequence)', 0.05) }} AS inventory_trend

FROM inventory_metrics