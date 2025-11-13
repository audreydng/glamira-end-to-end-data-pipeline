with stg_summary as (
    select * from {{ ref('stg_summary') }}
),

distinct_sessions as (
    select distinct
        ip_address,
        user_agent,
        resolution,
        referrer_url,
        current_url,
        store_id,
        api_version
    from stg_summary
    where ip_address is not null or user_agent is not null -- filter out completely null rows
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'ip_address', 
            'user_agent', 
            'resolution', 
            'referrer_url', 
            'current_url',
            'store_id',
            'api_version'
        ]) }} as session_context_key,

        ip_address,
        user_agent,
        resolution,
        referrer_url,
        current_url,
        store_id,
        api_version
    from distinct_sessions
)

select * from final