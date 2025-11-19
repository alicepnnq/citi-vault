{{ config(materialized='view', schema='staging', alias='stg_station_information') }}

 -- La colonne JSON s’appelle doc. On la caste en jsonb et on l’appelle payload.
with src as (
  select doc::jsonb as payload
  from {{ source('raw', 'gbfs_station_information') }}
),

 -- { "data": { "stations": [ {...}, {...} ] } }


unnested as (
  -- payload -> 'data' -> 'stations' : on navigue jusqu’au tableau des stations.
  -- jsonb_array_elements(...) : explose ce tableau → retourne une ligne par élément (ici, une station).
  -- 	as s : chaque ligne a une colonne s (type jsonb) = 1 station.
  -- {"station_id":"A1","name":"Fort Hamilton","lat":40.64,...}

  select jsonb_array_elements(payload -> 'data' -> 'stations') as s
  from src
),

methods as (
  -- pour les champs rental_method !!
  -- on déplie rental_methods et on les ré-agrège
  -- Pour chaque station u.s, on prend la clé rental_methods (un tableau JSON).
	-- jsonb_array_elements_text(...) : explode ce tableau → une ligne par méthode (m = texte).
	-- array_agg(m) : ré-agrège en tableau SQL (text[]).
  select
    (u.s ->> 'short_name') as short_name,
    array_agg(m)::text[]   as rental_methods
  from unnested u
  cross join lateral jsonb_array_elements_text(u.s -> 'rental_methods') as m
  group by 1
)
select
  u.s ->> 'station_id'                              as station_id,
  u.s ->> 'name'                                    as name,
  nullif(u.s ->> 'short_name','')                   as short_name,
  (u.s ->> 'lat')::double precision                 as lat,
  (u.s ->> 'lon')::double precision                 as lon,
  nullif(u.s ->> 'region_id','')                    as region_id,
  nullif(u.s ->> 'capacity','')::int                as capacity,
  nullif(u.s ->> 'station_type','')                 as station_type,
  (u.s ->> 'has_kiosk')::boolean                    as has_kiosk,
  (u.s ->> 'eightd_has_key_dispenser')::boolean     as eightd_has_key_dispenser,
  (u.s ->> 'electric_bike_surcharge_waiver')::boolean as electric_bike_surcharge_waiver,
  coalesce(m.rental_methods, array[]::text[])       as rental_methods,
  u.s -> 'rental_uris' ->> 'android'                as rental_uri_android,
  u.s -> 'rental_uris' ->> 'ios'                    as rental_uri_ios
from unnested u
left join methods m on u.s ->> 'short_name' = m.short_name
where u.s ->> 'short_name' is not null
