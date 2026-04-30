WITH customers AS (
    SELECT * FROM {{ ref('stg_customer') }}
),

address AS (
    SELECT 
        ca_address_sk AS addr_id,
        ca_city AS city,
        ca_state AS state,
        ca_zip AS zip_code
    FROM {{ source('tpcds_raw', 'CUSTOMER_ADDRESS') }}
),

customer_demo AS (
    SELECT 
        cd_demo_sk AS cdemo_id,
        cd_gender AS gender,
        cd_marital_status AS marital_status,
        cd_education_status AS education_status
    FROM {{ source('tpcds_raw', 'CUSTOMER_DEMOGRAPHICS') }}
),

household_demo AS (
    SELECT 
        hd_demo_sk AS hdemo_id,
        hd_income_band_sk AS income_band_id,
        hd_buy_potential AS buy_potential
    FROM {{ source('tpcds_raw', 'HOUSEHOLD_DEMOGRAPHICS') }}
)

SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email_address,
    c.age_in_2000,
    c.age_group,
    a.city,
    a.state,
    a.zip_code,
    cd.gender,
    cd.marital_status,
    cd.education_status,
    hd.buy_potential
FROM customers c
LEFT JOIN address a ON c.current_addr_id = a.addr_id
LEFT JOIN customer_demo cd ON c.current_cdemo_id = cd.cdemo_id
LEFT JOIN household_demo hd ON c.current_hdemo_id = hd.hdemo_id