# Step 4 — Pipeline Results

## 1. Pipeline Execution

The ELT pipeline ran successfully with all 11 Luigi tasks completing without errors:

```
===== Luigi Execution Summary =====
Scheduled 11 tasks of which:
* 11 ran successfully:
    - 1 AllExtractLoad()
    - 1 DbtRun()
    - 1 DbtSnapshot()
    - 1 ExtractLoadAircrafts()
    - 1 ExtractLoadAirlines()
    - 1 ExtractLoadAirports()
    - 1 ExtractLoadCustomers()
    - 1 ExtractLoadFlightBookings()
    - 1 ExtractLoadHotelBookings()
    - 1 ExtractLoadHotel()
    - 1 MasterPipeline()
This progress looks :) because there were no failed tasks or missing dependencies
===== Luigi Execution Summary =====
```

Pipeline flow: **Extract & Load (7 tables)** → **dbt snapshot** (SCD Type 2 dim_customer) → **dbt run** (dimensions + facts)

## 2. Table Row Counts

| Table | Row Count |
| --- | --- |
| dim_date | 3317 |
| dim_customer | 1000 |
| dim_airline | 1251 |
| dim_aircraft | 246 |
| dim_airport | 105 |
| dim_hotel | 1470 |
| fct_flight_bookings | 8190 |
| fct_hotel_bookings | 217 |
| fct_daily_bookings | 2548 |

All 9 DWH tables in the `final` schema are populated with data.

## 3. Business Requirement Queries & Results

### a) Business Requirement 1 — Track Daily Booking Volumes

**Query:**
```sql
SELECT
    d.full_date,
    f.booking_type,
    f.total_bookings,
    f.total_revenue,
    f.avg_price
FROM final.fct_daily_bookings f
JOIN final.dim_date d ON f.date_key = d.date_key
ORDER BY d.full_date, f.booking_type
LIMIT 20;
```

**Results (first 20 rows):**

| Full Date | Booking Type | Total Bookings | Total Revenue | Avg Price |
| --- | --- | --- | --- | --- |
| 2010-01-02 | flight | 1 | 131 | 131.00 |
| 2010-01-07 | flight | 1 | 467 | 467.00 |
| 2010-01-08 | flight | 1 | 30 | 30.00 |
| 2010-01-11 | flight | 1 | 122 | 122.00 |
| 2010-01-16 | flight | 1 | 73 | 73.00 |
| 2010-01-21 | flight | 1 | 531 | 531.00 |
| 2010-01-26 | flight | 3 | 1069 | 356.33 |
| 2010-01-27 | flight | 2 | 1203 | 601.50 |
| 2010-01-28 | flight | 2 | 103 | 51.50 |
| 2010-02-07 | flight | 1 | 48 | 48.00 |
| 2010-02-10 | flight | 1 | 255 | 255.00 |
| 2010-02-12 | flight | 1 | 673 | 673.00 |
| 2010-02-20 | flight | 1 | 444 | 444.00 |
| 2010-02-20 | hotel | 1 | 3120 | 3120.00 |
| 2010-02-23 | flight | 1 | 696 | 696.00 |
| 2010-02-25 | flight | 1 | 128 | 128.00 |
| 2010-02-26 | flight | 2 | 1555 | 777.50 |
| 2010-02-28 | flight | 1 | 363 | 363.00 |
| 2010-03-02 | flight | 1 | 48 | 48.00 |
| 2010-03-05 | flight | 1 | 511 | 511.00 |

**Explanation:** The `fct_daily_bookings` periodic snapshot table aggregates daily booking volumes by type (flight/hotel). This directly meets **Business Requirement 1** by providing a daily view of booking counts, total revenue, and average price for each booking type.

### b) Business Requirement 2 — Monitor Average Ticket Prices Over Time

**Query:**
```sql
SELECT
    d.year,
    d.month_name,
    d.month,
    AVG(f.price) AS avg_ticket_price,
    COUNT(*) AS total_bookings
FROM final.fct_flight_bookings f
JOIN final.dim_date d ON f.departure_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

**Results (sample — first 15 months):**

| Year | Month | Month # | Avg Ticket Price | Total Bookings |
| --- | --- | --- | --- | --- |
| 2010 | January   | 1 | 286.85 | 13 |
| 2010 | February  | 2 | 462.44 | 9 |
| 2010 | March     | 3 | 367.32 | 25 |
| 2010 | April     | 4 | 390.08 | 13 |
| 2010 | May       | 5 | 526.56 | 18 |
| 2010 | June      | 6 | 356.35 | 17 |
| 2010 | July      | 7 | 489.35 | 20 |
| 2010 | August    | 8 | 450.79 | 14 |
| 2010 | September | 9 | 381.08 | 13 |
| 2010 | October   | 10 | 501.42 | 12 |
| 2010 | November  | 11 | 445.16 | 25 |
| 2010 | December  | 12 | 377.80 | 20 |
| 2011 | January   | 1 | 390.48 | 31 |
| 2011 | February  | 2 | 321.05 | 19 |
| 2011 | March     | 3 | 333.53 | 19 |

**Explanation:** This query joins `fct_flight_bookings` with `dim_date` to calculate monthly average ticket prices. This directly meets **Business Requirement 2** by showing how flight prices fluctuate over time, enabling trend analysis and pricing strategy decisions.

### c) Top 10 Airlines by Booking Volume

**Query:**
```sql
SELECT
    a.airline_name, a.country,
    COUNT(*) AS total_bookings,
    AVG(f.price) AS avg_price
FROM final.fct_flight_bookings f
JOIN final.dim_airline a ON f.airline_key = a.airline_key
GROUP BY a.airline_name, a.country
ORDER BY total_bookings DESC
LIMIT 10;
```

**Results:**

| Airline | Country | Total Bookings | Avg Price |
| --- | --- | --- | --- |
| Ryanair | Ireland | 500 | 349.52 |
| easyJet | United Kingdom | 274 | 324.72 |
| Scandinavian Airlines System | Sweden | 266 | 302.18 |
| LAN Airlines | Chile | 223 | 349.19 |
| Turkish Airlines | Turkey | 212 | 385.44 |
| Tunisair | Tunisia | 192 | 349.74 |
| Austrian Airlines | Austria | 188 | 337.74 |
| World Scale Airlines | United States | 178 | 297.76 |
| United Airlines | United States | 174 | 580.87 |
| US Airways | United States | 173 | 523.73 |

### d) Top 10 Hotels by Revenue

**Query:**
```sql
SELECT
    h.hotel_name, h.city, h.country, h.hotel_score,
    COUNT(*) AS total_bookings,
    SUM(f.price) AS total_revenue,
    AVG(f.stay_duration_days) AS avg_stay_days
FROM final.fct_hotel_bookings f
JOIN final.dim_hotel h ON f.hotel_key = h.hotel_key
GROUP BY h.hotel_name, h.city, h.country, h.hotel_score
ORDER BY total_revenue DESC
LIMIT 10;
```

**Results:**

| Hotel | City | Country | Score | Bookings | Revenue | Avg Stay (days) |
| --- | --- | --- | --- | --- | --- | --- |
| InterContinental London Park Lane | London | United Kingdom | 8.5 | 2 | 6480 | 27.0 |
| Hotel Die Port van Cleve | Amsterdam | Netherlands | 8.0 | 2 | 5760 | 24.0 |
| Arbor City | London | United Kingdom | 8.9 | 2 | 5400 | 22.5 |
| Dikker en Thijs Fenice Hotel | Amsterdam | Netherlands | 8.4 | 2 | 5400 | 22.5 |
| Falkensteiner Hotel Wien Margareten | Vienna | Austria | 8.9 | 2 | 4920 | 20.5 |
| Sofitel London St James | London | United Kingdom | 9.2 | 2 | 4920 | 20.5 |
| BEST WESTERN PLUS Amedia Wien | Vienna | Austria | 8.4 | 2 | 4800 | 20.0 |
| FourSide Hotel Vienna City Center | Vienna | Austria | 8.1 | 2 | 4680 | 19.5 |
| Simply Rooms Suites | London | United Kingdom | 7.7 | 2 | 4590 | 25.5 |
| Radisson Blu Edwardian New Providence Wharf | London | United Kingdom | 9.0 | 2 | 4560 | 19.0 |

### e) Bookings by Travel Class

**Query:**
```sql
SELECT
    f.travel_class,
    COUNT(*) AS total_bookings,
    AVG(f.price) AS avg_price,
    MIN(f.price) AS min_price,
    MAX(f.price) AS max_price
FROM final.fct_flight_bookings f
GROUP BY f.travel_class
ORDER BY total_bookings DESC;
```

**Results:**

| Travel Class | Total Bookings | Avg Price | Min Price | Max Price |
| --- | --- | --- | --- | --- |
| business | 4126 | 577.51 | 402 | 1643 |
| economy | 4064 | 171.39 | 2 | 1085 |

### f) Top 10 Routes (Source → Destination)

**Query:**
```sql
SELECT
    src.city AS source_city,
    dst.city AS destination_city,
    COUNT(*) AS total_flights,
    AVG(f.price) AS avg_price
FROM final.fct_flight_bookings f
JOIN final.dim_airport src ON f.airport_src_key = src.airport_key
JOIN final.dim_airport dst ON f.airport_dst_key = dst.airport_key
GROUP BY src.city, dst.city
ORDER BY total_flights DESC
LIMIT 10;
```

**Results:**

| Source City | Destination City | Total Flights | Avg Price |
| --- | --- | --- | --- |
| Mirnyj | Moscow | 8 | 272.88 |
| Moscow | Saratov | 8 | 299.75 |
| Gelendzhik | Ulyanovsk | 7 | 408.43 |
| Yakutsk | Pskov | 6 | 348.50 |
| Murmansk | Tomsk | 6 | 338.83 |
| Astrakhan | Pskov | 6 | 490.83 |
| Moscow | Kaluga | 6 | 328.83 |
| Moscow | Ulyanovsk | 6 | 449.67 |
| Nyagan | Uraj | 6 | 388.67 |
| Yakutsk | Ulyanovsk | 6 | 325.67 |

### g) SCD Type 2 Verification — dim_customer

**Query:**
```sql
SELECT customer_key, customer_id, customer_first_name, customer_family_name,
       customer_country, effective_date, expiry_date, is_current
FROM final.dim_customer
ORDER BY customer_id
LIMIT 10;
```

**Results:**

| Customer Key | Customer ID | First Name | Family Name | Country | Effective Date | Expiry Date | Is Current |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 12801 | Marie Annick | VIEMONT | Germany | 2026-03-20 05:49:40.735746 | None | True |
| 2 | 16419 | Christiane | BELLARD | Austria | 2026-03-20 05:49:40.735746 | None | True |
| 3 | 39098 | Bernardette | PIOU | Colombia | 2026-03-20 05:49:40.735746 | None | True |
| 4 | 40241 | Bachir | AUDIGANE | Japan | 2026-03-20 05:49:40.735746 | None | True |
| 5 | 44330 | Marie Hélène | RIPOCHE | United States of America | 2026-03-20 05:49:40.735746 | None | True |
| 6 | 59700 | Léon | PITHON | Iran | 2026-03-20 05:49:40.735746 | None | True |
| 7 | 73552 | Vinciane | ARNOU | Austria | 2026-03-20 05:49:40.735746 | None | True |
| 8 | 86899 | Charline | LAURENDEAU | Japan | 2026-03-20 05:49:40.735746 | None | True |
| 9 | 92645 | Denise | COGNEE | Austria | 2026-03-20 05:49:40.735746 | None | True |
| 10 | 111049 | Samir | MECHINEAU | India | 2026-03-20 05:49:40.735746 | None | True |

**Explanation:** The `dim_customer` table is implemented as an **SCD Type 2** dimension via dbt snapshot. Each row has `effective_date`, `expiry_date`, and `is_current` columns to track historical changes. When a customer's attributes change, the old record gets an `expiry_date` and a new record is created with `is_current = True`.

## 4. Key Insights

1. **Most Popular Airline:** Ryanair leads with 500 bookings, making it the most booked airline on the platform.

2. **Top Revenue Hotel:** InterContinental London Park Lane generated the highest hotel revenue at $6480, indicating strong demand.

3. **Travel Class Distribution:** business class dominates with 4126 bookings at an average price of $577.51, while economy class has 4064 bookings at $171.39 average.

4. **Busiest Route:** Mirnyj → Moscow is the most popular route with 8 total flights.

5. **Price Trends:** Monthly average ticket prices range from $262.89 (May      ) to $526.56 (May      ), showing notable seasonal variation in flight pricing.
