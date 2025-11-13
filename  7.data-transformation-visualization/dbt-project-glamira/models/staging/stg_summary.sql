with source_summary as (
    select * from {{ source('glamira_raw_data', 'summary') }}
)
select
    -- Timestamps
    TIMESTAMP_SECONDS(time_stamp) as event_timestamp,
    local_time,

    -- Keys
    safe_cast(user_id_db as string) as customer_source_id,
    safe_cast(product_id as string) as product_source_id,
    safe_cast(order_id as string) as order_source_id,
    device_id,
    ip as ip_address,

    -- Session info
    user_agent,
    resolution,
    referrer_url,
    current_url,
    store_id,
    api_version,

    -- Customer info
    email_address,

    -- Order info
    safe_cast(price as float64) as price,
    currency

from source_summary
