WITH staging_store AS (
    SELECT SUM(sales_price * quantity) AS total_staging
    FROM {{ ref('stg_store_sales') }}
),

mart_store AS (
    SELECT SUM(gross_revenue) AS total_mart
    FROM {{ ref('mart_sales_by_channel') }}
    WHERE sales_channel = 'Store'
)

SELECT 
    s.total_staging, 
    m.total_mart
FROM staging_store s
CROSS JOIN mart_store m
-- Tolerancia de 0.01 para evitar fallos por redondeos de punto flotante
WHERE ABS(s.total_staging - m.total_mart) > 0.01