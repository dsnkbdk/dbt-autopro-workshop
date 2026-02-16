with 

source as (

    select * from {{ source('autopro_workshop', 'vehicles') }}

),

dedup as (

    select
        vehicle_id,
        customer_id,
        make,
        model,
        year,
        rego,
        odometer,
        color,
        _etl_loaded_at,
        _etl_file_name,
        _etl_row_number

    from source

    qualify row_number() over (
        partition by vehicle_id
        order by _etl_loaded_at desc, _etl_file_name desc, _etl_row_number desc
    ) = 1

)

select * from dedup