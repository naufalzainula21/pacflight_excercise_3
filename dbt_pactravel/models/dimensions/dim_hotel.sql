{{
    config(
        materialized='table'
    )
}}

select
    row_number() over (order by hotel_id) as hotel_key,
    hotel_id,
    hotel_name,
    hotel_address,
    city,
    country,
    hotel_score
from {{ ref('stg_hotel') }}
