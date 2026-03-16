with source as (

    select * from analytics_warehouse.staging.stg_pg_dbalab_products

),

renamed as (

    select
        product_id::integer         as product_id,
        product_name,
        category,
        price::numeric(12,2)        as unit_price,
        _source_engine,
        _source_database,
        _loaded_at

    from source

    where product_id is not null

)

select * from renamed