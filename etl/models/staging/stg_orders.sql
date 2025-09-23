with source as (
    select *
    from {{ ref('orders') }}
),
renamed as (
    select
        cast(order_id as integer) as order_id,
        cast(customer_id as integer) as customer_id,
        cast(order_date as date) as order_date,
        cast(order_amount as decimal(10, 2)) as order_amount,
        lower(trim(order_status)) as order_status
    from source
)
select *
from renamed