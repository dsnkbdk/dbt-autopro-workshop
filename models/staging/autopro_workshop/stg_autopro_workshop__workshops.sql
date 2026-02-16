with 

source as (

    select * from {{ source('autopro_workshop', 'workshops') }}

),

renamed as (

    select *
    from source

)

select * from renamed