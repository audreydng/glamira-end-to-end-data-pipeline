with source_product as (
    select
        product_id,
        product_name,
        price as list_price,
        currency as currency_code,
        category,
        category_path,
        description,
        image_url,
        rating,
        crawled_at as insert_date
    from {{ source('glamira_raw_data', 'product_details') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id as product_source_id,
    product_name,
    list_price,
    currency_code,
    category,
    category_path,
    description,
    image_url,
    rating,
    cast(insert_date as timestamp) as insert_timestamp
from source_product
