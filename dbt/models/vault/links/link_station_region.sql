{{ config(
    materialized='incremental',
    schema='vault',
    alias='link_station_region',
    unique_key='l_station_region_hashkey',
    on_schema_change='ignore'
) }}

with src as (
  select
    region_id::text as region_id,
    short_name::text  as short_name
  from {{ ref('stg_station_information') }}
  where short_name is not null
    and region_id  is not null
)

, hashed as (
  select
      md5(upper(trim(short_name))) as h_station_hashkey,
      md5(upper(trim(region_id)))  as h_region_hashkey,
      md5(
        upper(
          trim(
            md5(upper(trim(short_name))) || '-' ||
            md5(upper(trim(region_id)))
          )
        )
      ) as l_station_region_hashkey,
      now()                        as load_dts,
      'stg_station_information'    as record_source
  from src
)

select * from hashed

{% if is_incremental() %}
where l_station_region_hashkey not in (
    select l_station_region_hashkey from {{ this }}
)
{% endif %}
