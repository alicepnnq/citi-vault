{{ config(
    materialized='incremental',
    schema='vault',
    alias='sat_trip_details',
    unique_key='h_trip_hashkey',
    on_schema_change='ignore'
) }}

with src as (
    select
        md5(upper(trim(trip_id))) as h_trip_hashkey,

        rideable_type,
        started_at,
        ended_at,
        start_station_name,
        end_station_name,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual

    from {{ ref('stg_trips') }}
)

select
    h_trip_hashkey,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    now() as load_dts,
    'stg_trips' as record_source

from src

{% if is_incremental() %}
  where h_trip_hashkey not in (
    select h_trip_hashkey from {{ this }}
  )
{% endif %}
