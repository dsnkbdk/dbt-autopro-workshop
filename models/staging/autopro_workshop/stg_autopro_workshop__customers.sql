with 

source as (

    select * from {{ source('autopro_workshop', 'customers') }}

),

renamed as (

    select *
    from source

)

select * from renamed