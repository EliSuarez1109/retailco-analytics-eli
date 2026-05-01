SELECT 
    customer_id, 
    total_clv, 
    total_transactions
FROM {{ ref('mart_customer_lifetime_value') }}
WHERE total_clv > 0 
  AND total_transactions = 0