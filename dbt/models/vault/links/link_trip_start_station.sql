{{ config(
    materialized='incremental',
    schema='vault',
    alias='link_trip_start_station',
    unique_key='l_trip_start_station_hashkey',
    on_schema_change='ignore'
) }}

with src as (
  select
    trip_id::text as trip_id,
    start_station_id::text  as start_station_id
  from {{ ref('stg_trips') }}
  where trip_id is not null
    and start_station_id  is not null
)

, hashed as (
  select
      md5(upper(trim(trip_id))) as h_trip_hashkey,
      md5(upper(trim(start_station_id)))  as h_start_station_hashkey,
      md5(
        upper(
          trim(
            md5(upper(trim(trip_id))) || '-' ||
            md5(upper(trim(start_station_id)))
          )
        )
      ) as l_trip_start_station_hashkey,
      now()                        as load_dts,
      'stg_trips'    as record_source
  from src
)

select * from hashed

{% if is_incremental() %}
where l_trip_start_station_hashkey not in (
    select l_trip_start_station_hashkey from {{ this }}
)
{% endif %}
