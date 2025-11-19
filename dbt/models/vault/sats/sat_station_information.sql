{{ config(
    materialized='incremental',
    schema='vault',
    alias='sat_station_information',
    incremental_strategy='append',
    on_schema_change='sync_all_columns'
) }}


with src as (
  select
    short_name::text as short_name,
    station_id::text as station_id,
    name::text,
    lat::float,
    lon::float,
    station_type::text,
    electric_bike_surcharge_waiver::boolean,
    has_kiosk::boolean,
    rental_methods::text[],
    now() as load_dts,
    'stg_station_information' as record_source
  from {{ ref('stg_station_information') }}
)

, hashed as (
  select
    md5(upper(trim(short_name))) as h_station_hashkey,
    station_id,
    name,
    lat,
    lon,
    station_type,
    electric_bike_surcharge_waiver,
    has_kiosk,
    rental_methods,
    load_dts,
    record_source,
    md5(
      coalesce(station_id::text, '') ||
      coalesce(name, '') ||
      coalesce(lat::text, '') ||
      coalesce(lon::text, '') ||
      coalesce(station_type, '') ||
      coalesce(electric_bike_surcharge_waiver::text, '') ||
      coalesce(has_kiosk::text, '') ||
      coalesce(array_to_string(rental_methods, ','), '')
    ) as hash_diff
  from src
)

select *
from hashed

{% if is_incremental() %}
where hash_diff not in (
  select hash_diff from {{ this }}
)
{% endif %}
