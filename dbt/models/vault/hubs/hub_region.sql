{{ config(
    materialized='incremental',
    schema='vault',
    alias='hub_region',
    unique_key='h_region_hashkey',
    on_schema_change='ignore'
) }}

with src as (
  select distinct
    region_id::text as region_id
  from {{ ref('stg_system_regions') }} as ssi
  where region_id is not null
)

select
  md5(upper(trim(region_id))) as h_region_hashkey,
  region_id                   as bk_region_id,
  now()                        as load_dts,
  'stg_system_regions'    as record_source
from src

{% if is_incremental() %}
where md5(upper(trim(region_id))) not in (
  select h_region_hashkey from {{ this }}
)
{% endif %}
