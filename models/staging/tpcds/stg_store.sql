WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'STORE') }}
)
SELECT
    CAST(s_store_sk AS NUMBER) AS store_id,
    CAST(s_store_id AS VARCHAR) AS store_alternate_id,
    CAST(s_store_name AS VARCHAR) AS store_name,
    CAST(s_number_employees AS NUMBER) AS number_employees,
    CAST(s_floor_space AS NUMBER) AS floor_space,
    CAST(s_hours AS VARCHAR) AS hours,
    CAST(s_manager AS VARCHAR) AS manager_name,
    CAST(s_city AS VARCHAR) AS city,
    CAST(s_state AS VARCHAR) AS state,
    CAST(s_zip AS VARCHAR) AS zip_code,
    CAST(s_country AS VARCHAR) AS country,
    
    -- Fechas
    CAST(s_rec_start_date AS DATE) AS valid_from,
    CAST(s_rec_end_date AS DATE) AS valid_to,
    
    -- Columna Derivada: Vigencia
    CASE WHEN s_rec_end_date IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM source