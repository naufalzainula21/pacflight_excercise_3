# Step 2 - Designing Data Warehouse Model

## 2.1 Select Business Process

The business processes to model are:

1. **Flight Booking** — A customer books a flight on an airline, with a specific aircraft, from a source airport to a destination airport, on a given date, at a certain price and travel class.

2. **Hotel Booking** — A customer books a hotel stay for a date range at a certain price.

These two processes directly support the analytical needs:
- **Track Daily Booking Volumes** (both flight and hotel)
- **Monitor Average Ticket Prices Over Time** (flight and hotel prices)

---

## 2.2 Declare Grain

| Fact Table | Grain | Description |
|-----------|-------|-------------|
| `fct_flight_bookings` | One row per flight booking per seat | Each record represents a single seat booked on a flight by a customer (composite key: trip_id, flight_number, seat_number) |
| `fct_hotel_bookings` | One row per hotel booking | Each record represents a single hotel reservation by a customer (key: trip_id) |
| `fct_daily_bookings` | One row per day per booking type | Each record represents the aggregated count and average price of bookings for a given day and booking type (flight or hotel) |

---

## 2.3 Identify Dimensions

### dim_customer
Captures customer demographic information for segmenting bookings.

| Column | Type | Description |
|--------|------|-------------|
| `customer_key` | SERIAL (PK) | Surrogate key |
| `customer_id` | INT | Natural key from source |
| `customer_first_name` | VARCHAR | First name |
| `customer_family_name` | VARCHAR | Family name |
| `customer_gender` | VARCHAR | Gender |
| `customer_birth_date` | DATE | Date of birth |
| `customer_country` | VARCHAR | Country of residence |
| `customer_phone_number` | BIGINT | Phone number |
| `effective_date` | DATE | SCD Type 2: start date |
| `expiry_date` | DATE | SCD Type 2: end date |
| `is_current` | BOOLEAN | SCD Type 2: current flag |

### dim_airline
Captures airline reference data.

| Column | Type | Description |
|--------|------|-------------|
| `airline_key` | SERIAL (PK) | Surrogate key |
| `airline_id` | INT | Natural key from source |
| `airline_name` | VARCHAR | Airline name |
| `country` | VARCHAR | Country of origin |
| `airline_iata` | VARCHAR | IATA code |
| `airline_icao` | VARCHAR | ICAO code |
| `alias` | VARCHAR | Alternative name |

### dim_aircraft
Captures aircraft reference data.

| Column | Type | Description |
|--------|------|-------------|
| `aircraft_key` | SERIAL (PK) | Surrogate key |
| `aircraft_id` | VARCHAR | Natural key from source |
| `aircraft_name` | VARCHAR | Aircraft name |
| `aircraft_iata` | VARCHAR | IATA code |
| `aircraft_icao` | VARCHAR | ICAO code |

### dim_airport
Captures airport reference data.

| Column | Type | Description |
|--------|------|-------------|
| `airport_key` | SERIAL (PK) | Surrogate key |
| `airport_id` | INT | Natural key from source |
| `airport_name` | VARCHAR | Airport name |
| `city` | VARCHAR | City |
| `latitude` | FLOAT | Latitude |
| `longitude` | FLOAT | Longitude |

### dim_hotel
Captures hotel reference data.

| Column | Type | Description |
|--------|------|-------------|
| `hotel_key` | SERIAL (PK) | Surrogate key |
| `hotel_id` | INT | Natural key from source |
| `hotel_name` | VARCHAR | Hotel name |
| `hotel_address` | VARCHAR | Address |
| `city` | VARCHAR | City |
| `country` | VARCHAR | Country |
| `hotel_score` | FLOAT | Rating score |

### dim_date
Role-playing date dimension used for departure_date, check_in_date, check_out_date, and daily aggregations.

| Column | Type | Description |
|--------|------|-------------|
| `date_key` | INT (PK) | Surrogate key (YYYYMMDD format) |
| `full_date` | DATE | Actual date |
| `day_of_week` | INT | Day of week (1=Monday) |
| `day_name` | VARCHAR | Day name (Monday, etc.) |
| `day_of_month` | INT | Day of month |
| `week_of_year` | INT | ISO week number |
| `month` | INT | Month number |
| `month_name` | VARCHAR | Month name |
| `quarter` | INT | Quarter (1-4) |
| `year` | INT | Year |
| `is_weekend` | BOOLEAN | Weekend flag |

---

## 2.4 Identify Facts

### Fact Table 1: `fct_flight_bookings` (Transaction Fact Table)

Captures every individual flight booking at the most granular level. Supports price trend analysis.

| Column | Type | Description |
|--------|------|-------------|
| `flight_booking_key` | SERIAL (PK) | Surrogate key |
| `trip_id` | INT | Source trip ID |
| `flight_number` | VARCHAR | Flight number |
| `seat_number` | VARCHAR | Seat number |
| `customer_key` | INT (FK) | -> dim_customer |
| `airline_key` | INT (FK) | -> dim_airline |
| `aircraft_key` | INT (FK) | -> dim_aircraft |
| `airport_src_key` | INT (FK) | -> dim_airport (source) |
| `airport_dst_key` | INT (FK) | -> dim_airport (destination) |
| `departure_date_key` | INT (FK) | -> dim_date |
| `departure_time` | TIME | Departure time |
| `flight_duration` | VARCHAR | Duration |
| `travel_class` | VARCHAR | Travel class (economy, business, etc.) |
| `price` | INT | Ticket price (measure) |

**Measures**: `price`

### Fact Table 2: `fct_hotel_bookings` (Transaction Fact Table)

Captures every individual hotel booking at the most granular level.

| Column | Type | Description |
|--------|------|-------------|
| `hotel_booking_key` | SERIAL (PK) | Surrogate key |
| `trip_id` | INT | Source trip ID |
| `customer_key` | INT (FK) | -> dim_customer |
| `hotel_key` | INT (FK) | -> dim_hotel |
| `check_in_date_key` | INT (FK) | -> dim_date (check-in) |
| `check_out_date_key` | INT (FK) | -> dim_date (check-out) |
| `price` | INT | Booking price (measure) |
| `breakfast_included` | BOOLEAN | Breakfast flag |
| `stay_duration_days` | INT | Number of nights (derived measure) |

**Measures**: `price`, `stay_duration_days`

### Fact Table 3: `fct_daily_bookings` (Periodic Snapshot Fact Table)

Aggregates booking activity per day, supporting daily volume tracking.

| Column | Type | Description |
|--------|------|-------------|
| `daily_booking_key` | SERIAL (PK) | Surrogate key |
| `date_key` | INT (FK) | -> dim_date |
| `booking_type` | VARCHAR | 'flight' or 'hotel' |
| `total_bookings` | INT | Count of bookings that day |
| `total_revenue` | BIGINT | Sum of prices that day |
| `avg_price` | NUMERIC | Average price that day |

**Measures**: `total_bookings`, `total_revenue`, `avg_price`

---

## 2.5 Fact Table Types Summary

| Fact Table | Type | Purpose |
|-----------|------|---------|
| `fct_flight_bookings` | **Transaction** | Individual flight bookings for granular price analysis |
| `fct_hotel_bookings` | **Transaction** | Individual hotel bookings for granular analysis |
| `fct_daily_bookings` | **Periodic Snapshot** | Daily aggregated metrics for volume tracking |

This design uses **2 types** of fact tables as required:
1. **Transaction Fact Tables** (`fct_flight_bookings`, `fct_hotel_bookings`) — one row per business event
2. **Periodic Snapshot Fact Table** (`fct_daily_bookings`) — one row per time period per booking type

---

## 2.6 SCD (Slowly Changing Dimension) Strategy

| Dimension | SCD Type | Rationale |
|-----------|----------|-----------|
| `dim_customer` | **Type 2** | Customer attributes (name, country, phone) may change over time. We need to preserve history to accurately reflect which customer profile was active at the time of each booking. New rows are inserted with `effective_date`, `expiry_date`, and `is_current` columns. |
| `dim_airline` | **Type 1** | Airline reference data rarely changes. If it does (e.g., name correction), overwrite in place — no history needed. |
| `dim_aircraft` | **Type 1** | Aircraft data is static reference data. Overwrite on change. |
| `dim_airport` | **Type 1** | Airport data is mostly static. Overwrite on change. |
| `dim_hotel` | **Type 1** | Hotel attributes (score, address) change infrequently. Overwrite with latest values — only current state matters. |
| `dim_date` | **N/A** | Date dimension is pre-generated and immutable. No SCD needed. |

---

## 2.7 ERD (Entity Relationship Diagram)

```
                            ┌──────────────┐
                            │  dim_date    │
                            │──────────────│
                            │ date_key (PK)│
                            │ full_date    │
                            │ day_of_week  │
                            │ day_name     │
                            │ day_of_month │
                            │ week_of_year │
                            │ month        │
                            │ month_name   │
                            │ quarter      │
                            │ year         │
                            │ is_weekend   │
                            └──────┬───────┘
                                   │
            ┌──────────────────────┼──────────────────────┐
            │                      │                      │
            v                      v                      v
    ┌───────────────┐    ┌─────────────────┐    ┌──────────────────┐
    │fct_daily_     │    │fct_flight_      │    │fct_hotel_        │
    │bookings       │    │bookings         │    │bookings          │
    │───────────────│    │─────────────────│    │──────────────────│
    │daily_booking_ │    │flight_booking_  │    │hotel_booking_    │
    │  key (PK)     │    │  key (PK)       │    │  key (PK)        │
    │date_key (FK)  │    │trip_id          │    │trip_id           │
    │booking_type   │    │flight_number    │    │customer_key (FK) │
    │total_bookings │    │seat_number      │    │hotel_key (FK)    │
    │total_revenue  │    │customer_key(FK) │    │check_in_date_    │
    │avg_price      │    │airline_key (FK) │    │  key (FK)        │
    └───────────────┘    │aircraft_key(FK) │    │check_out_date_   │
                         │airport_src_     │    │  key (FK)        │
                         │  key (FK)       │    │price             │
                         │airport_dst_     │    │breakfast_included│
                         │  key (FK)       │    │stay_duration_days│
                         │departure_date_  │    └────────┬─────────┘
                         │  key (FK)       │             │
                         │departure_time   │             │
                         │flight_duration  │       ┌─────┴─────┐
                         │travel_class     │       │           │
                         │price            │       v           v
                         └──┬──┬──┬──┬─────┘  ┌────────┐ ┌──────────┐
                            │  │  │  │        │dim_    │ │dim_hotel │
                            │  │  │  │        │customer│ │──────────│
                            │  │  │  │        │────────│ │hotel_key │
                            │  │  │  │        │customer│ │hotel_id  │
                            │  │  │  │        │ _key   │ │hotel_name│
                            │  │  │  │        │customer│ │hotel_    │
                            │  │  │  │        │ _id    │ │ address  │
                            │  │  │  │        │first_  │ │city      │
                            │  │  │  │        │ name   │ │country   │
                            │  │  │  │        │family_ │ │hotel_    │
                            │  │  │  │        │ name   │ │ score    │
                            │  │  │  │        │gender  │ └──────────┘
                            │  │  │  │        │birth_  │
                            │  │  │  │        │ date   │
                            │  │  │  │        │country │
                            │  │  │  │        │phone   │
                            │  │  │  │        │eff_date│
                            │  │  │  │        │exp_date│
                            │  │  │  │        │is_curr │
                            │  │  │  │        └────────┘
                            │  │  │  │
                 ┌──────────┘  │  │  └──────────┐
                 v             │  │             v
          ┌────────────┐       │  │      ┌────────────┐
          │dim_airport │       │  │      │dim_airport │
          │(source)    │       │  │      │(destination│
          │────────────│       │  │      │────────────│
          │airport_key │       │  │      │airport_key │
          │airport_id  │       │  │      │airport_id  │
          │airport_name│       │  │      │airport_name│
          │city        │       │  │      │city        │
          │latitude    │       │  │      │latitude    │
          │longitude   │       │  │      │longitude   │
          └────────────┘       │  │      └────────────┘
                               │  │
                    ┌──────────┘  └──────────┐
                    v                        v
             ┌────────────┐          ┌──────────────┐
             │dim_airline │          │dim_aircraft  │
             │────────────│          │──────────────│
             │airline_key │          │aircraft_key  │
             │airline_id  │          │aircraft_id   │
             │airline_name│          │aircraft_name │
             │country     │          │aircraft_iata │
             │airline_iata│          │aircraft_icao │
             │airline_icao│          └──────────────┘
             │alias       │
             └────────────┘
```

### Star Schema Summary

The design follows a **star schema** pattern:
- **3 fact tables** at the center (2 transaction + 1 periodic snapshot)
- **6 dimension tables** surrounding them
- `dim_airport` is a **role-playing dimension** — used twice in `fct_flight_bookings` (source & destination)
- `dim_date` is a **role-playing dimension** — used for departure_date, check_in_date, check_out_date, and daily snapshot date
- `dim_customer` uses **SCD Type 2** for historical tracking
- All other dimensions use **SCD Type 1** (overwrite)

### Sample Analytical Queries

**1. Track Daily Booking Volumes:**
```sql
SELECT
    d.full_date,
    f.booking_type,
    f.total_bookings,
    f.total_revenue,
    f.avg_price
FROM final.fct_daily_bookings f
JOIN final.dim_date d ON f.date_key = d.date_key
ORDER BY d.full_date, f.booking_type;
```

**2. Monitor Average Ticket Prices Over Time:**
```sql
SELECT
    d.year,
    d.month_name,
    a.airline_name,
    AVG(f.price) AS avg_ticket_price,
    COUNT(*) AS total_bookings
FROM final.fct_flight_bookings f
JOIN final.dim_date d ON f.departure_date_key = d.date_key
JOIN final.dim_airline a ON f.airline_key = a.airline_key
GROUP BY d.year, d.month, d.month_name, a.airline_name
ORDER BY d.year, d.month;
```
