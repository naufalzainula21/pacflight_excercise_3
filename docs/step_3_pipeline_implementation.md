# Step 3 - Data Pipeline Implementation

## 3.1 Overview

The PacTravel ELT pipeline is implemented using **Python + Luigi** for Extract & Load and **dbt** for Transform. The pipeline extracts data from 7 source tables, loads them into a staging schema, and transforms them into a star-schema data warehouse.

### Pipeline Flow

```
Source DB (port 5433)           DWH DB (port 5434)
┌──────────────────┐    EL     ┌───────────────────┐   dbt    ┌──────────────────┐
│  public schema   │ ───────►  │ pactravel schema  │ ──────►  │  final schema    │
│  (7 tables)      │  Luigi    │ (staging mirror)  │          │ (star schema)    │
└──────────────────┘           └───────────────────┘          └──────────────────┘
```

---

## 3.2 Extract & Load (Python + Luigi)

### Database Connections (`src/extract_load/db_connections.py`)

Two connection helpers using `psycopg2`, with credentials loaded from `.env` via `python-dotenv`:
- `get_source_conn()` — connects to Source DB (port 5433)
- `get_dwh_conn()` — connects to DWH DB (port 5434)

### EL Tasks (`src/extract_load/tasks.py`)

Each of the 7 source tables has a dedicated Luigi task class:

| Luigi Task | Source Table | Staging Table |
|-----------|-------------|---------------|
| `ExtractLoadAircrafts` | `public.aircrafts` | `pactravel.aircrafts` |
| `ExtractLoadAirlines` | `public.airlines` | `pactravel.airlines` |
| `ExtractLoadAirports` | `public.airports` | `pactravel.airports` |
| `ExtractLoadCustomers` | `public.customers` | `pactravel.customers` |
| `ExtractLoadHotel` | `public.hotel` | `pactravel.hotel` |
| `ExtractLoadFlightBookings` | `public.flight_bookings` | `pactravel.flight_bookings` |
| `ExtractLoadHotelBookings` | `public.hotel_bookings` | `pactravel.hotel_bookings` |

### EL Process (per table)

1. Fetch column metadata from source `information_schema`
2. Extract all rows from the source table
3. `CREATE TABLE IF NOT EXISTS` on DWH (auto-creates staging table from source metadata)
4. `TRUNCATE` the staging table
5. Bulk insert rows using `executemany`

### Completion Tracking

Each task writes a marker file (`temp/el_<table>.done`) upon success. Luigi uses these as `LocalTarget` outputs to determine if a task needs re-running.

---

## 3.3 Transform (dbt)

### dbt Project Structure

```
dbt_pactravel/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Connection profile (env vars)
├── snapshots/
│   └── snap_dim_customer.sql    # SCD Type 2 snapshot
└── models/
    ├── staging/
    │   ├── _src_pactravel.yml   # Source definitions
    │   ├── stg_aircrafts.sql
    │   ├── stg_airlines.sql
    │   ├── stg_airports.sql
    │   ├── stg_customers.sql
    │   ├── stg_hotel.sql
    │   ├── stg_flight_bookings.sql
    │   └── stg_hotel_bookings.sql
    ├── dimensions/
    │   ├── dim_date.sql
    │   ├── dim_airline.sql
    │   ├── dim_aircraft.sql
    │   ├── dim_airport.sql
    │   ├── dim_customer.sql
    │   └── dim_hotel.sql
    └── facts/
        ├── fct_flight_bookings.sql
        ├── fct_hotel_bookings.sql
        └── fct_daily_bookings.sql
```

### Staging Models

Materialized as **views**. Each staging model is a simple `SELECT *` from the corresponding source table in the `pactravel` schema, providing a clean abstraction layer.

### Dimension Models

Materialized as **tables** in the `final` schema:

| Model | SCD Type | Description |
|-------|----------|-------------|
| `dim_date` | N/A | Pre-generated date spine covering all dates in the data |
| `dim_customer` | Type 2 | dbt snapshot with `check` strategy on all columns |
| `dim_airline` | Type 1 | Surrogate key via `ROW_NUMBER()` |
| `dim_aircraft` | Type 1 | Surrogate key via `ROW_NUMBER()` |
| `dim_airport` | Type 1 | Surrogate key via `ROW_NUMBER()` |
| `dim_hotel` | Type 1 | Surrogate key via `ROW_NUMBER()` |

### dim_customer (SCD Type 2)

Implemented as a **dbt snapshot** (`snap_dim_customer.sql`):
- Strategy: `check` (monitors all columns for changes)
- Unique key: `customer_id`
- Tracking columns: `dbt_valid_from`, `dbt_valid_to`
- The `dim_customer` model then selects from the snapshot and renames columns to `effective_date`, `expiry_date`, `is_current`

### Fact Models

| Model | Type | Grain | Key Measures |
|-------|------|-------|-------------|
| `fct_flight_bookings` | Transaction | One row per flight booking per seat | `price` |
| `fct_hotel_bookings` | Transaction | One row per hotel booking | `price`, `stay_duration_days` |
| `fct_daily_bookings` | Periodic Snapshot | One row per day per booking type | `total_bookings`, `total_revenue`, `avg_price` |

Fact tables join with dimension tables to resolve surrogate keys (e.g., `stg_flight_bookings.airline_id` joins with `dim_airline.airline_id` to get `airline_key`).

---

## 3.4 Orchestration (Luigi Master Pipeline)

### Pipeline DAG (`src/pipeline.py`)

```
ExtractLoadAircrafts  ──┐
ExtractLoadAirlines   ──┤
ExtractLoadAirports   ──┤
ExtractLoadCustomers  ──┼──► AllExtractLoad ──► DbtSnapshot ──► DbtRun ──► MasterPipeline
ExtractLoadHotel      ──┤
ExtractLoadFlightBookings ──┤
ExtractLoadHotelBookings ──┘
```

- `AllExtractLoad` — Luigi `WrapperTask` that requires all 7 EL tasks
- `DbtSnapshot` — runs `dbt snapshot` after EL completes
- `DbtRun` — runs `dbt run` after snapshot completes
- `MasterPipeline` — entry point that requires `DbtRun`

### Luigi Configuration (`luigi.cfg`)

- Log level: INFO
- Task history recording: disabled
- Worker keep-alive: disabled

---

## 3.5 Scheduling

A shell script (`run_pipeline.sh`) is provided for cron-based scheduling:

```bash
# Crontab entry — daily at midnight
0 0 * * * /path/to/pactravel-pipeline/run_pipeline.sh >> /path/to/logs/cron.log 2>&1
```

The script:
- Changes to the project directory
- Creates a timestamped log file
- Activates virtual environment if present
- Runs the Luigi master pipeline
- Logs success/failure status

---

## 3.6 Alerting

Pipeline failure alerting is implemented in `src/alert.py`:

- Uses Luigi's `@event_handler(luigi.Event.FAILURE)` decorator
- On task failure: logs to `logs/alerts.log` with timestamp, task name, parameters, and error message
- On task success: logs info-level message

### Alert Log Format

```
[2026-03-20 05:49:30 UTC] TASK FAILURE | task=ExtractLoadFlightBookings params={} | error=relation "pactravel.flight_bookings" does not exist
```

---

## 3.7 Running the Pipeline

```bash
# 1. Start Docker containers
cd pactravel-dataset && docker compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure credentials
cp .env.example .env

# 4. Run the pipeline
cd pactravel-pipeline
rm -f temp/*.done
python -m luigi --module src.pipeline MasterPipeline --local-scheduler
```

### Expected Output

```
===== Luigi Execution Summary =====
Scheduled 11 tasks of which:
* 11 ran successfully
This progress looks :) because there were no failed tasks or missing dependencies
===== Luigi Execution Summary =====
```
