-- stg_orders.sql
-- LEARNING NOTE:
-- This is a staging model — 1-to-1 from the source staging table.
-- We do light cleaning only: rename columns, cast types, filter nulls.
-- No joins, no business logic — that belongs in marts.
-- never hardcode table names.select * from analytics_warehouse.staging.stg_pg_dbalab_orders
-- The 'source()' macro references raw staging tables loaded by our pipeline.

with source as (

    -- Pull raw data from the staging table loaded by our ELT pipeline
    -- stg_pg_dbalab_orders has the best structure — customers, products, orders related
    select * from analytics_warehouse.staging.stg_pg_dbalab_orders

),

renamed as (

    select
        -- Rename and cast columns to standard names
        order_id::bigint                                    as order_id,
        customer_id::integer                                as customer_id,
        product_id::integer                                 as product_id,
        quantity::integer                                   as quantity,
        price::numeric(12, 2)                               as unit_price,
        status                                              as order_status,
        order_time::timestamp                               as ordered_at,

        -- Derived column — total value of this order line
        (quantity::integer * price::numeric)::numeric(14,2) as order_total,

        -- Audit columns from our pipeline — keep for lineage
        _source_engine,
        _source_database,
        _loaded_at

    from source

    -- Filter out rows with no order_id or customer_id — bad data
    where order_id is not null
      and customer_id is not null

)

select * from renamed