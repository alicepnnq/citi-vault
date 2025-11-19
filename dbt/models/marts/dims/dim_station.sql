{{ config(
    materialized='table',
    schema='marts',
    alias='dim_station'
) }}

with hub as (
    select
        h_station_hashkey,
        bk_station_id as station_id
    from {{ ref('hub_station') }}
),

sat as (
    select
        h_station_hashkey,
        name as station_name,
        lat,
        lon,
        station_type,
        has_kiosk
    from {{ ref('sat_station_information') }}
),

link_region as (
    select
        h_station_hashkey,
        h_region_hashkey
    from {{ ref('link_station_region') }}
),

region as (
    select
        h_region_hashkey,
        bk_region_id as region_id
    from {{ ref('hub_region') }}
)

select
    hub.h_station_hashkey,
    hub.station_id,

    sat.station_name,
    sat.lat,
    sat.lon,
    sat.station_type,
    sat.has_kiosk,

    region.region_id

from hub
left join sat using (h_station_hashkey)
left join link_region using (h_station_hashkey)
left join region using (h_region_hashkey)
