with source as (

    select * from analytics_warehouse.staging.stg_pg_dbalab_customers

),

renamed as (

    select
        customer_id::integer    as customer_id,
        name                    as customer_name,
        city,
        created_at::timestamp   as created_at,
        _source_engine,
        _source_database,
        _loaded_at

    from source

    where customer_id is not null

)

select * from renamed