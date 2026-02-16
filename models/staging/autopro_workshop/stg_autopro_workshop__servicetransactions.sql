with 

source as (

    select * from {{ source('autopro_workshop', 'servicetransactions') }}

),

renamed as (

    select *
    from source

)

select * from renamed