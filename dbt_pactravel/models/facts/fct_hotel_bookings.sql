{{
    config(
        materialized='table'
    )
}}

with hb as (
    select * from {{ ref('stg_hotel_bookings') }}
),

dim_customer as (
    select customer_key, customer_id
    from {{ ref('dim_customer') }}
    where is_current = true
),

dim_hotel as (
    select hotel_key, hotel_id
    from {{ ref('dim_hotel') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
)

select
    row_number() over (order by hb.trip_id)    as hotel_booking_key,
    hb.trip_id,
    c.customer_key,
    h.hotel_key,
    ci.date_key                                as check_in_date_key,
    co.date_key                                as check_out_date_key,
    hb.price,
    hb.breakfast_included,
    (hb.check_out_date - hb.check_in_date)     as stay_duration_days
from hb
left join dim_customer c
    on hb.customer_id = c.customer_id
left join dim_hotel h
    on hb.hotel_id = h.hotel_id
left join dim_date ci
    on hb.check_in_date = ci.full_date
left join dim_date co
    on hb.check_out_date = co.full_date
