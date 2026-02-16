with 

source as (

    select * from {{ source('autopro_workshop', 'technicians') }}

),

dedup as (

    select
        technician_id,
        first_name,
        last_name,
        workshop_id,
        qualification_level,
        hourly_rate,
        hire_date,
        _etl_loaded_at,
        _etl_file_name,
        _etl_row_number

    from source

    qualify row_number() over (
        partition by technician_id
        order by _etl_loaded_at desc, _etl_file_name desc, _etl_row_number desc
    ) = 1

)

select * from dedup