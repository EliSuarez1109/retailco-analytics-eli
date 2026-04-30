WITH source AS (
    SELECT * FROM {{ source('tpcds_raw', 'CALL_CENTER') }}
)
SELECT
    CAST(cc_call_center_sk AS NUMBER) AS call_center_id,
    CAST(cc_call_center_id AS VARCHAR) AS call_center_alternate_id,
    CAST(cc_name AS VARCHAR) AS call_center_name,
    CAST(cc_class AS VARCHAR) AS call_center_class,
    CAST(cc_employees AS NUMBER) AS number_of_employees,
    CAST(cc_sq_ft AS NUMBER) AS square_feet,
    CAST(cc_city AS VARCHAR) AS city,
    CAST(cc_state AS VARCHAR) AS state
    
FROM source