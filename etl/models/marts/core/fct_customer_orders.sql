with orders as (
    select *
    from {{ ref('stg_orders') }}
    where order_status <> 'cancelled'
),
customers as (
    select *
    from {{ ref('stg_customers') }}
),
joined as (
    select
        o.order_id,
        o.customer_id,
        c.customer_name,
        c.customer_email,
        o.order_date,
        o.order_amount,
        o.order_status
    from orders o
    left join customers c using (customer_id)
),
aggregated as (
    select
        customer_id,
        customer_name,
        customer_email,
        count(*) as order_count,
        sum(order_amount) as total_revenue,
        sum(case when order_status = 'completed' then order_amount else 0 end) as completed_revenue,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from joined
    group by 1,2,3
)
select *
from aggregated