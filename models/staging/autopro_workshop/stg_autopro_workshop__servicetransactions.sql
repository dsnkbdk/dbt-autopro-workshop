with 

source as (

    select * from {{ source('autopro_workshop', 'servicetransactions') }}

),

dedup as (

    select
        transaction_id,
        transaction_date,
        workshop_id,
        customer_id,
        vehicle_id,
        technician_id,
        service_type_id,
        labour_hours,
        labour_cost,
        parts_cost,
        total_cost,
        payment_method,
        status,
        modified_date,
        _etl_loaded_at,
        _etl_file_name,
        _etl_row_number

    from source
    
    qualify row_number() over (
        partition by transaction_id
        order by modified_date desc, _etl_loaded_at desc, _etl_file_name desc, _etl_row_number desc
    ) = 1

)

select * from dedup