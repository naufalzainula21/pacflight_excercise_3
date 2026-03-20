{{
    config(
        materialized='table'
    )
}}

select
    row_number() over (order by airport_id) as airport_key,
    airport_id,
    airport_name,
    city,
    latitude,
    longitude
from {{ ref('stg_airports') }}
