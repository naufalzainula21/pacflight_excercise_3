{{
    config(
        materialized='table'
    )
}}

select
    row_number() over (order by aircraft_id) as aircraft_key,
    aircraft_id,
    aircraft_name,
    aircraft_iata,
    aircraft_icao
from {{ ref('stg_aircrafts') }}
