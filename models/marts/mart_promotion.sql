{{ config(materialized='table') }}

SELECT 
    p.promo_name,
    p.promo_cost,
    COUNT(DISTINCT s.order_id) AS total_promo_orders,
    SUM(s.net_paid) AS promo_revenue,
    
    IFF(p.promo_cost > 0, ((SUM(s.net_paid) - p.promo_cost) / p.promo_cost) * 100, 0) AS promo_roi_pct,
    
    p.channel_tv,
    p.channel_email
FROM {{ ref('int_unified_sales') }} s
JOIN {{ ref('stg_promotion') }} p ON s.promo_id = p.promo_id
WHERE s.promo_id IS NOT NULL 
  AND p.promo_name != 'Unk' 
GROUP BY 1, 2, 6, 7