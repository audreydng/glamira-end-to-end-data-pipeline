with stg_summary as (
    select *
    from {{ ref('stg_summary') }}
    where order_source_id is not null
),

-- Reference all dimension tables
dim_product as (
    select * from {{ ref('dim_product') }}
),
dim_customer as (
    select * from {{ ref('dim_customer') }}
),
dim_location as (
    select * from {{ ref('dim_location') }}
),
dim_date as (
    select * from {{ ref('dim_date') }}
),
dim_session_context as (
    select * from {{ ref('dim_session_context') }}
),

-- Join staging with dimensions to form the fact table
final as (
    select
        -- Surrogate Key for Fact
        {{ dbt_utils.generate_surrogate_key([
            "cast(stg.order_source_id as string)",
            "cast(stg.product_source_id as string)"
        ]) }} as sales_order_key,
        
        -- Foreign Keys
        coalesce(dp.product_key, '-1') as product_key,
        coalesce(dc.customer_key, '-1') as customer_key,
        coalesce(dl.location_key, '-1') as location_key,
        coalesce(dsc.session_context_key, '-1') as session_context_key,
        coalesce(dd.date_key, -1) as date_key,  -- -1 for unknown dates (int64)
        
        -- Measures
        stg.price as sales_amount,
        
        -- Degenerate Dimensions & Attributes
        stg.order_source_id,
        stg.ip_address,
        stg.local_time,
        stg.currency,
        stg.event_timestamp as order_timestamp
        
    from stg_summary as stg
    
    -- Join Product Dimension
    left join dim_product as dp
        on cast(stg.product_source_id as string) = cast(dp.product_source_id as string)
    
    -- Join Customer Dimension
    left join dim_customer as dc
        on cast(stg.customer_source_id as string) = cast(dc.customer_source_id as string)
    
    -- Join Location Dimension
    left join dim_location as dl
        on cast(stg.ip_address as string) = cast(dl.ip_address as string)
    
    -- Join Date Dimension
    left join dim_date as dd
        on cast(stg.event_timestamp as date) = cast(dd.full_date as date)
    
    -- Join Session Context (composite key)
    left join dim_session_context as dsc
        on cast(stg.ip_address as string) = cast(dsc.ip_address as string)
        and cast(stg.user_agent as string) = cast(dsc.user_agent as string)
        and cast(stg.resolution as string) = cast(dsc.resolution as string)
        and cast(stg.referrer_url as string) = cast(dsc.referrer_url as string)
        and cast(stg.current_url as string) = cast(dsc.current_url as string)
        and cast(stg.store_id as string) = cast(dsc.store_id as string)
        and cast(stg.api_version as string) = cast(dsc.api_version as string)
)

-- Output
select *
from final
