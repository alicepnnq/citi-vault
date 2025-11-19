{{ config(
    materialized='view',
    schema='marts',
    alias='fct_trips'
) }}

with trips as (

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
        member_casual
    from {{ ref('sat_trip_informations') }}

),

start_station as (
    select
        h_station_hashkey,
        station_id as start_station_id,
        station_name as start_station_name_clean,
        lat as start_station_lat,
        lon as start_station_lng
    from {{ ref('dim_station') }}
),

end_station as (
    select
        h_station_hashkey,
        station_id as end_station_id,
        station_name as end_station_name_clean,
        lat as end_station_lat,
        lon as end_station_lng
    from {{ ref('dim_station') }}
),

links_start as (
    select
        h_trip_hashkey,
        h_start_station_hashkey
    from {{ ref('link_trip_start_station') }}
),

links_end as (
    select
        h_trip_hashkey,
        h_end_station_hashkey
    from {{ ref('link_trip_end_station') }}
)

select
    t.h_trip_hashkey,
    t.rideable_type,
    t.started_at,
    t.ended_at,
    extract(epoch from (t.ended_at - t.started_at)) / 60 as duration_minutes,

    ls.h_start_station_hashkey,
    ss.start_station_id,
    ss.start_station_name_clean,
    ss.start_station_lat,
    ss.start_station_lng,

    le.h_end_station_hashkey,
    es.end_station_id,
    es.end_station_name_clean,
    es.end_station_lat,
    es.end_station_lng,

    t.member_casual

from trips t
left join links_start ls
    on t.h_trip_hashkey = ls.h_trip_hashkey
left join start_station ss
    on ls.h_start_station_hashkey = ss.h_station_hashkey

left join links_end le
    on t.h_trip_hashkey = le.h_trip_hashkey
left join end_station es
    on le.h_end_station_hashkey = es.h_station_hashkey
