with 

source as (

    select * from {{ source('autopro_workshop', 'customers') }}

),

dedup as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        address,
        city,
        postcode,
        preferred_workshop,
        _etl_loaded_at,
        _etl_file_name,
        _etl_row_number

    from source

    qualify row_number() over (
        partition by customer_id
        order by _etl_loaded_at desc, _etl_file_name desc, _etl_row_number desc
    ) = 1

)

select * from dedup