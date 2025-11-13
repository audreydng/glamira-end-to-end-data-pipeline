with stg_summary as (
    select distinct
        customer_source_id,
        email_address,
        device_id,
        event_timestamp
    from {{ ref('stg_summary') }}
    where customer_source_id is not null
)
select
    {{ dbt_utils.generate_surrogate_key(['customer_source_id']) }} as customer_key,
    customer_source_id,
    email_address,
    device_id,
    cast(event_timestamp as date) as insert_date -- first seen date
from stg_summary
-- Newest record for each customer_source_id
qualify row_number() over (partition by customer_source_id order by event_timestamp desc) = 1
