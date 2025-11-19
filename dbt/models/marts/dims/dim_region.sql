{{ config(
    materialized='table',
    alias='dim_region'
) }}

with hub as (
    select
        h_region_hashkey,
        bk_region_id as region_id
    from {{ ref('hub_region') }}
),

sat as (
    select
        h_region_hashkey,
        region_name
    from {{ ref('sat_region_information') }}
)

select
    hub.region_id,
    sat.region_name
from hub
left join sat using (h_region_hashkey)
