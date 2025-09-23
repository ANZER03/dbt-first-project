with source as (
    select *
    from {{ ref('customers') }}
),
renamed as (
    select
        cast(customer_id as integer) as customer_id,
        trim(customer_name) as customer_name,
        lower(trim(customer_email)) as customer_email
    from source
)
select *
from renamed