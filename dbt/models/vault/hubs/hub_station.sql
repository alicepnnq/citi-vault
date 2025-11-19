{{ config(
    materialized='incremental',
    schema='vault',
    alias='hub_station',
    unique_key='h_station_hashkey',
    on_schema_change='ignore'
) }}

with src as (
  select distinct
    short_name ::text as short_name
  from {{ ref('stg_station_information') }} as ssi
  where short_name is not null
)

select
  md5(upper(trim(short_name))) as h_station_hashkey,
  short_name                  as bk_station_id,
  now()                        as load_dts,
  'stg_station_information'    as record_source
from src

{% if is_incremental() %}
where md5(upper(trim(short_name))) not in (
  select h_station_hashkey from {{ this }}
)
{% endif %}
