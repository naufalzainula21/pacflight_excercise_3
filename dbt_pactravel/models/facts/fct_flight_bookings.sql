{{
    config(
        materialized='table'
    )
}}

with fb as (
    select * from {{ ref('stg_flight_bookings') }}
),

dim_customer as (
    select customer_key, customer_id
    from {{ ref('dim_customer') }}
    where is_current = true
),

dim_airline as (
    select airline_key, airline_id
    from {{ ref('dim_airline') }}
),

dim_aircraft as (
    select aircraft_key, aircraft_id
    from {{ ref('dim_aircraft') }}
),

dim_airport as (
    select airport_key, airport_id
    from {{ ref('dim_airport') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
)

select
    row_number() over (
        order by fb.trip_id, fb.flight_number, fb.seat_number
    )                                      as flight_booking_key,
    fb.trip_id,
    fb.flight_number,
    fb.seat_number,
    c.customer_key,
    al.airline_key,
    ac.aircraft_key,
    src_ap.airport_key                     as airport_src_key,
    dst_ap.airport_key                     as airport_dst_key,
    dd.date_key                            as departure_date_key,
    fb.departure_time,
    fb.flight_duration,
    fb.travel_class,
    fb.price
from fb
left join dim_customer c
    on fb.customer_id = c.customer_id
left join dim_airline al
    on fb.airline_id = al.airline_id
left join dim_aircraft ac
    on fb.aircraft_id = ac.aircraft_id
left join dim_airport src_ap
    on fb.airport_src = src_ap.airport_id
left join dim_airport dst_ap
    on fb.airport_dst = dst_ap.airport_id
left join dim_date dd
    on fb.departure_date = dd.full_date
