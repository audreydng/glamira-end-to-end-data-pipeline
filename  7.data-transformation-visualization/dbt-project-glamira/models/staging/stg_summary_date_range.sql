{% set source_relation = source('glamira_raw_data', 'summary') %}

with partitions as (
    select
        min(partition_id) as min_partition,
        max(partition_id) as max_partition
    from {{ adapter.quote(source_relation.database) }}.{{ adapter.quote(source_relation.schema) }}.INFORMATION_SCHEMA.PARTITIONS
    where table_name = '{{ source_relation.name }}'
      and partition_id != '__NULL__'
),
parsed as (
    select
        safe.parse_date('%Y%m%d', min_partition) as raw_start,
        safe.parse_date('%Y%m%d', max_partition) as raw_end
    from partitions
)
select
    -- If NULL take current date
    coalesce(raw_start, current_date()) as start_date,
    coalesce(raw_end, current_date()) as end_date
from parsed
