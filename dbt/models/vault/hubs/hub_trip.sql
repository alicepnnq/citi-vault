{{ config(
    materialized='incremental',
    schema='vault',
    alias='hub_trip',
    unique_key='h_trip_hashkey',
    on_schema_change='ignore'
) }}

with src as (
    select distinct
        trip_id
    from {{ ref('stg_trips') }}
    where trip_id is not null
)

select
    md5(upper(trim(trip_id))) as h_trip_hashkey,
    trip_id                   as bk_trip_id,
    now()                     as load_dts,
    'stg_trips'               as record_source
from src

{% if is_incremental() %}
  where md5(upper(trim(trip_id))) not in (
    select h_trip_hashkey from {{ this }}
  )
{% endif %}
