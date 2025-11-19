{{ config(materialized='view', schema='staging', alias='stg_system_regions') }}

with src as (
  select doc::jsonb as payload
  from {{ source('raw', 'gbfs_system_regions') }}
),
unnested as (
  select jsonb_array_elements(payload -> 'data' -> 'regions') as r
  from src
)
select
  r ->> 'region_id' as region_id,
  r ->> 'name'      as region_name
from unnested
