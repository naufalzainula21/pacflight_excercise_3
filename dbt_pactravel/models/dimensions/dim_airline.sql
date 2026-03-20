{{
    config(
        materialized='table'
    )
}}

select
    row_number() over (order by airline_id) as airline_key,
    airline_id,
    airline_name,
    country,
    airline_iata,
    airline_icao,
    alias
from {{ ref('stg_airlines') }}
