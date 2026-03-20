{{
    config(
        materialized='table'
    )
}}

/*
  Periodic snapshot: one row per (date, booking_type) aggregating
  total bookings, total revenue, and average price.
*/

with flight_daily as (
    select
        departure_date_key  as date_key,
        'flight'            as booking_type,
        count(*)            as total_bookings,
        sum(price)          as total_revenue,
        avg(price)          as avg_price
    from {{ ref('fct_flight_bookings') }}
    group by departure_date_key
),

hotel_daily as (
    select
        check_in_date_key   as date_key,
        'hotel'             as booking_type,
        count(*)            as total_bookings,
        sum(price)          as total_revenue,
        avg(price)          as avg_price
    from {{ ref('fct_hotel_bookings') }}
    group by check_in_date_key
),

combined as (
    select * from flight_daily
    union all
    select * from hotel_daily
)

select
    row_number() over (order by date_key, booking_type) as daily_booking_key,
    date_key,
    booking_type,
    total_bookings,
    total_revenue,
    avg_price
from combined
