WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'CUSTOMER') }}
)
SELECT
    CAST(c_customer_sk AS NUMBER) AS customer_id,
    CAST(c_customer_id AS VARCHAR) AS customer_alternate_id,
    CAST(c_current_cdemo_sk AS NUMBER) AS current_cdemo_id,
    CAST(c_current_hdemo_sk AS NUMBER) AS current_hdemo_id,
    CAST(c_current_addr_sk AS NUMBER) AS current_addr_id,
    
    -- Limpieza de nombres nulos
    COALESCE(CAST(c_first_name AS VARCHAR), 'Unknown') AS first_name,
    COALESCE(CAST(c_last_name AS VARCHAR), 'Unknown') AS last_name,
    COALESCE(CAST(c_first_name AS VARCHAR), '') || ' ' || COALESCE(CAST(c_last_name AS VARCHAR), '') AS full_name,
    
    CAST(c_email_address AS VARCHAR) AS email_address,
    
    -- Columna Derivada: Edad aproximada TENIENDO EN CUENTA EL AÑO DEL DATA SET QUE ES EL 2000
    (2000 - c_birth_year) AS age_in_2000,
    
    -- Clasificación simple derivada
    CASE 
        WHEN (2000 - c_birth_year) < 30 THEN 'Young Adult'
        WHEN (2000 - c_birth_year) BETWEEN 30 AND 55 THEN 'Adult'
        ELSE 'Senior' 
    END AS age_group
    
FROM source