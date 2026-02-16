with 

source as (

    select * from {{ source('autopro_workshop', 'servicetypes') }}

),

renamed as (

    select
        service_type_id,
        service_type_name,
        category,
        _etl_loaded_at,
        _etl_file_name,
        _etl_row_number

    from source

)

select * from renamed