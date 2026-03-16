with source as (

    select * from analytics_warehouse.staging.stg_pg_hr_employees

),

renamed as (

    select
        emp_id::integer         as employee_id,
        emp_name                as employee_name,
        department,
        salary::numeric(14,2)   as salary,
        hire_date::date         as hire_date,
        _source_engine,
        _source_database,
        _loaded_at

    from source

    where emp_id is not null

)

select * from renamed