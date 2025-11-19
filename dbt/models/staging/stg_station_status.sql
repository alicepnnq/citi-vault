{{ config(materialized='view', schema='staging', alias='stg_station_status') }}

-- 1) On lit la table brute déclarée en source.
 -- La colonne JSON s’appelle doc. On la caste en jsonb et on l’appelle payload.
with src as (
  select doc::jsonb as payload
  from {{ source('raw', 'gbfs_station_status') }}
),
 -- { "data": { "stations": [ {...}, {...} ] } }


-- 2) On déplie le tableau JSON data.stations : 1 ligne = 1 station (colonne s = jsonb).

unnested as (
  -- payload -> 'data' -> 'stations' : on navigue jusqu’au tableau des stations.
  -- jsonb_array_elements(...) : explose ce tableau → retourne une ligne par élément (ici, une station).
  -- 	as s : chaque ligne a une colonne s (type jsonb) = 1 station.
  -- {"station_id":"A1","name":"Fort Hamilton","lat":40.64,...}

  select jsonb_array_elements(payload -> 'data' -> 'stations') as s
  from src
),


-- 3) On aplatit les champs utiles + on caste + on join avec stg_station_information our recup la feature short_name
status as (
  select
    u.s ->> 'station_id'                                as station_id,
    nullif(u.s ->> 'num_bikes_available','')::int       as num_bikes_available,
    nullif(u.s ->> 'num_bikes_disabled','')::int        as num_bikes_disabled,
    nullif(u.s ->> 'num_docks_available','')::int       as num_docks_available,
    nullif(u.s ->> 'num_docks_disabled','')::int        as num_docks_disabled,
    (u.s ->> 'is_installed')::boolean                   as is_installed,
    (u.s ->> 'is_renting')::boolean                     as is_renting,
    (u.s ->> 'is_returning')::boolean                   as is_returning,
    -- last_reported est un epoch en secondes dans GBFS
    to_timestamp(nullif(u.s ->> 'last_reported','')::bigint) as last_reported,
    coalesce((u.s ->> 'eightd_has_available_keys')::boolean, null) as eightd_has_available_keys
  from unnested u
  where u.s ->> 'station_id' is not null
),

joined as (
  select
    i.short_name,
    s.station_id,
    s.num_bikes_available,
    s.num_bikes_disabled,
    s.num_docks_available,
    s.num_docks_disabled,
    s.is_installed,
    s.is_renting,
    s.is_returning,
    s.last_reported,
    s.eightd_has_available_keys
  from status s
  left join {{ ref('stg_station_information') }} i
    on s.station_id = i.station_id
)

select
  short_name,
  station_id,
  num_bikes_available,
  num_bikes_disabled,
  num_docks_available,
  num_docks_disabled,
  is_installed,
  is_renting,
  is_returning,
  last_reported,
  eightd_has_available_keys
from joined
where short_name is not null
