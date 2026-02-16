with 

source as (

    select * from {{ source('autopro_workshop', 'servicetypes') }}

),

renamed as (

    select *
    from source

)

select * from renamed