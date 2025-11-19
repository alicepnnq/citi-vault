{{ config(materialized='view', schema='staging', alias='stg_trips') }}

SELECT
    trip_id,
    rideable_type,
    started_at::timestamp AS started_at,
    ended_at::timestamp AS ended_at,
    start_station_name,
    start_station_id::text AS start_station_id,
    end_station_name,
    end_station_id::text AS end_station_id,
    start_lat::float AS start_lat,
    start_lng::float AS start_lng,
    end_lat::float AS end_lat,
    end_lng::float AS end_lng,
    member_casual
FROM {{ source('raw', 'trips') }}
WHERE started_at::date < DATE '2024-01-03'
