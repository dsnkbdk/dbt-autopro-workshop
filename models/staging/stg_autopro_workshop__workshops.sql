with 

source as (

    select * from {{ source('autopro_workshop', 'workshops') }}

),

renamed as (

    select
        workshop_id,
        workshop_name,
        city,
        region,
        manager_name,
        _etl_loaded_at,
        _etl_file_name,
        _etl_row_number

    from source

)

select * from renamed