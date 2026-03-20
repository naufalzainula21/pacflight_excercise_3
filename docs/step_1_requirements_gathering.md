# Step 1 - Requirements Gathering

## 1.1 Data Source Description

**PacTravel** is a travel-domain transactional database stored in PostgreSQL, accessible via the repository: `https://github.com/Kurikulum-Sekolah-Pacmann/pactravel-dataset.git`.

The source database consists of **7 tables** covering flight and hotel booking operations:

### Source Tables

| Table | Rows | Description |
|-------|------|-------------|
| `aircrafts` | 246 | Aircraft master data (name, IATA/ICAO codes) |
| `airlines` | 1,251 | Airline master data (name, country, IATA/ICAO codes, alias) |
| `airports` | 105 | Airport master data (name, city, latitude, longitude) |
| `customers` | 1,000 | Customer profiles (name, gender, birth date, country, phone) |
| `hotel` | 1,470 | Hotel master data (name, address, city, country, score) |
| `flight_bookings` | 8,190 | Flight booking transactions (trip, customer, flight details, class, price) |
| `hotel_bookings` | 217 | Hotel booking transactions (trip, customer, hotel, dates, price) |

### Source Relationships

```
aircrafts ──┐
airlines  ──┤
airports  ──┤──> flight_bookings <── customers
airports  ──┘                           │
                                        v
hotel     ────> hotel_bookings  <── customers
```

- `flight_bookings` references: `customers`, `airlines`, `aircrafts`, `airports` (source & destination)
- `hotel_bookings` references: `customers`, `hotel`
- Primary key of `flight_bookings`: composite (`trip_id`, `flight_number`, `seat_number`)
- Primary key of `hotel_bookings`: `trip_id`

### Data Format & Infrastructure

- **Format**: PostgreSQL relational database
- **Infrastructure**: Dockerized with two PostgreSQL instances:
  - **Database 1 (Source)** — port `5433`: contains the raw transactional data (`public` schema)
  - **Database 2 (DWH)** — port `5434`: target data warehouse with `pactravel` (staging) and `final` (data warehouse) schemas

---

## 1.2 Problem Statement

The PacTravel operational database is designed for transactional processing (OLTP), which makes it inefficient for analytical queries. Stakeholders face the following challenges:

1. **No aggregated booking metrics** — There is no easy way to track daily booking volumes for flights and hotels. Analysts must write complex queries joining multiple tables and aggregating data on the fly.

2. **No historical price analysis** — Monitoring average ticket prices over time requires ad-hoc queries across the transactional system, which competes with production workloads and lacks historical tracking capabilities.

3. **No dimensional structure** — The normalized OLTP schema is not optimized for analytical queries. Joins across multiple tables (e.g., flight_bookings -> airlines -> airports -> customers) are expensive and slow for reporting purposes.

4. **No separation of concerns** — Running analytical queries directly on the production database risks degrading performance for operational users.

---

## 1.3 Proposed Solution

Build a **Data Warehouse** using a dimensional model (star schema) with an **ELT pipeline** to address the analytical needs:

### Architecture

```
[Source DB: pactravel]  --extract-->  [DWH Staging: pactravel schema]  --transform (dbt)-->  [DWH Final: final schema]
     (Port 5433)                              (Port 5434)                                       (Port 5434)
```

### Solution Components

1. **Staging Layer** (`pactravel` schema on DWH DB)
   - Mirror of source tables for decoupling from production
   - Loaded via Python Extract/Load scripts

2. **Data Warehouse Layer** (`final` schema on DWH DB)
   - Dimensional model with fact and dimension tables
   - Transformed using **dbt** (data build tool)
   - Optimized for the two core analytical needs:
     - **Track Daily Booking Volumes**: A periodic snapshot fact table aggregating daily counts of flight and hotel bookings
     - **Monitor Average Ticket Prices Over Time**: A transaction fact table capturing individual booking prices for trend analysis

3. **Orchestration & Scheduling**
   - **Luigi** for pipeline orchestration (Extract -> Load -> Transform)
   - Scheduled execution via cron or similar scheduler
   - Alerting on pipeline failures

### Expected Outcomes

| Analytical Need | Solution |
|----------------|----------|
| Track Daily Booking Volumes | `fct_daily_bookings` — periodic snapshot fact table with daily counts per booking type |
| Monitor Average Ticket Prices Over Time | `fct_flight_bookings` / `fct_hotel_bookings` — transaction fact tables with price per booking, joinable with `dim_date` for time-series analysis |
