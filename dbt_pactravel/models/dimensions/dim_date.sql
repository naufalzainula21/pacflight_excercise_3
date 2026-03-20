{{
    config(
        materialized='table'
    )
}}

/*
  Generate a date spine covering all departure_dates in flight_bookings
  and all check_in / check_out dates in hotel_bookings, plus a buffer.
  We derive the min/max dates dynamically from the staging tables.
*/

with date_bounds as (
    select
        least(
            min(departure_date),
            (select min(check_in_date) from {{ ref('stg_hotel_bookings') }})
        ) as min_date,
        greatest(
            max(departure_date),
            (select max(check_out_date) from {{ ref('stg_hotel_bookings') }})
        ) as max_date
    from {{ ref('stg_flight_bookings') }}
),

date_series as (
    select
        generate_series(
            (select min_date from date_bounds),
            (select max_date from date_bounds),
            interval '1 day'
        )::date as full_date
),

final as (
    select
        to_char(full_date, 'YYYYMMDD')::int       as date_key,
        full_date,
        extract(dow from full_date)::int           as day_of_week,
        to_char(full_date, 'Day')                  as day_name,
        extract(day from full_date)::int           as day_of_month,
        extract(week from full_date)::int          as week_of_year,
        extract(month from full_date)::int         as month,
        to_char(full_date, 'Month')                as month_name,
        extract(quarter from full_date)::int       as quarter,
        extract(year from full_date)::int          as year,
        case
            when extract(dow from full_date) in (0, 6) then true
            else false
        end                                        as is_weekend
    from date_series
)

select * from final
