{{ config(
    materialized='incremental',
    schema='vault',
    alias='sat_region_information',
    incremental_strategy='append',
    on_schema_change='ignore'
) }}

with src as (
  select
    region_id::text as region_id,
    region_name::text,
    now() as load_dts,
    'stg_system_regions' as record_source
  from {{ ref('stg_system_regions') }}
)

, hashed as (
  select
    md5(upper(trim(region_id))) as h_region_hashkey,
    region_name,
    load_dts,
    record_source,
    md5(coalesce(region_name, '')) as hash_diff
  from src
)

select *
from hashed

{% if is_incremental() %}
where hash_diff not in (
  select hash_diff from {{ this }}
)
{% endif %}
