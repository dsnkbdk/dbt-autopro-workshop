with 

source as (

    select * from {{ source('autopro_workshop', 'technicians') }}

),

renamed as (

    select *
    from source

)

select * from renamed