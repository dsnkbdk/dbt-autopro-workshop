with 

source as (

    select * from {{ source('autopro_workshop', 'vehicles') }}

),

renamed as (

    select *
    from source

)

select * from renamed