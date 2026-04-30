with source as (
    select * from {{ source('tpcds_raw', 'STORE') }}
),

renamed as (
    select
        s_store_sk as store_id,
        s_store_name as store_name,
        s_hours as store_hours
    from source
)

select * from renamed