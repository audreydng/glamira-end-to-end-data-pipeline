with source_location as (
    select
        ip_address,
        city_name,
        country_code,
        country_name,
        region_name,
        processed_at as insert_date 
    from {{ source('glamira_raw_data', 'ip_locations') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['ip_address']) }} as location_key,
    ip_address,
    city_name,
    country_code,
    country_name,
    region_name,
    TIMESTAMP_SECONDS(insert_date) as insert_timestamp
from source_location
-- Newest record for each ip_address
qualify row_number() over (partition by ip_address order by insert_date  desc) = 1

