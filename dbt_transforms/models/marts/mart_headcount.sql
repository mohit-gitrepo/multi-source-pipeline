-- mart_headcount.sql
-- Headcount and salary analytics by department.
-- References stg_employees staging model via ref().

with employees as (

    select * from {{ ref('stg_employees') }}

),

summary as (

    select
        department,
        count(employee_id)              as headcount,
        avg(salary)::numeric(14,2)      as avg_salary,
        min(salary)::numeric(14,2)      as min_salary,
        max(salary)::numeric(14,2)      as max_salary,
        sum(salary)::numeric(14,2)      as total_salary_cost,
        min(hire_date)                  as earliest_hire,
        max(hire_date)                  as latest_hire

    from employees

    group by 1

)

select * from summary