-- mart_sales_summary.sql
-- LEARNING NOTE:
-- Mart models join staging models together and apply business logic.
-- We use ref() here to reference our own staging models — not raw tables.
-- ref() tells dbt about the dependency — dbt runs stg_orders and
-- stg_customers BEFORE this model automatically.
-- Materialized as TABLE (not view) — data is physically stored,
-- queries against this mart are fast even at millions of rows.

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

joined as (

    select
        o.order_id,
        o.ordered_at,
        o.order_status,
        o.quantity,
        o.unit_price,
        o.order_total,

        -- Customer details
        c.customer_name,
        c.city                          as customer_city,

        -- Product details
        p.product_name,
        p.category                      as product_category

    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products p  on o.product_id  = p.product_id

),

summary as (

    select
        date_trunc('month', ordered_at)     as order_month,
        product_category,
        customer_city,
        order_status,
        count(order_id)                     as total_orders,
        sum(quantity)                       as total_quantity,
        sum(order_total)                    as total_revenue,
        avg(order_total)                    as avg_order_value,
        min(order_total)                    as min_order_value,
        max(order_total)                    as max_order_value

    from joined

    group by 1, 2, 3, 4

)

select * from summary