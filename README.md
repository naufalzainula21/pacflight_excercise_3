# PacTravel Data Pipeline

ELT pipeline for the PacTravel Data Warehouse project. Extracts data from a source PostgreSQL database, loads it into a staging schema, and transforms it into a star-schema data warehouse using dbt.

## Architecture

```
Source DB (port 5433)  ──►  Staging (pactravel schema)  ──►  DWH (final schema)
        │                          │                              │
   7 source tables           Luigi EL tasks                 dbt models
                            (truncate & load)          (dims + facts + snapshot)
```

## Tech Stack

- **Python 3.11** — scripting
- **Luigi** — workflow orchestration
- **psycopg2** — PostgreSQL connections
- **dbt (dbt-postgres)** — data transformations
- **Docker** — source & DWH databases

## Infrastructure

| Service   | Host      | Port | Database      |
|-----------|-----------|------|---------------|
| Source DB | localhost | 5433 | pactravel     |
| DWH DB   | localhost | 5434 | pactravel-dwh |

## DWH Schema

### Dimensions
- `dim_date` — date spine (pre-generated)
- `dim_customer` — SCD Type 2 (dbt snapshot)
- `dim_airline` — SCD Type 1
- `dim_aircraft` — SCD Type 1
- `dim_airport` — SCD Type 1
- `dim_hotel` — SCD Type 1

### Fact Tables
- `fct_flight_bookings` — transaction fact
- `fct_hotel_bookings` — transaction fact
- `fct_daily_bookings` — periodic snapshot (aggregated)

## Project Structure

```
pactravel-pipeline/
├── .env                          # DB credentials (not committed)
├── requirements.txt
├── luigi.cfg
├── run_pipeline.sh               # Cron-ready pipeline runner
├── src/
│   ├── __init__.py
│   ├── extract_load/
│   │   ├── __init__.py
│   │   ├── db_connections.py     # DB connection helpers
│   │   └── tasks.py              # Luigi EL tasks (7 tables)
│   ├── pipeline.py               # Master Luigi pipeline
│   └── alert.py                  # Failure alerting handler
└── dbt_pactravel/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── snapshots/
    │   └── snap_dim_customer.sql
    └── models/
        ├── staging/              # Source references (views)
        ├── dimensions/           # Dimension tables
        └── facts/                # Fact tables
```

## Setup

1. **Start databases** (from the dataset repo):
   ```bash
   docker compose up -d
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure credentials**:
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

4. **Run the pipeline**:
   ```bash
   rm -f temp/*.done
   python -m luigi --module src.pipeline MasterPipeline --local-scheduler
   ```

## Pipeline Flow

1. **Extract & Load** — 7 Luigi tasks run (can be parallel), each extracts from source DB and loads into `pactravel` staging schema
2. **dbt Snapshot** — runs `dbt snapshot` for SCD Type 2 `dim_customer`
3. **dbt Run** — builds all dimension and fact models in `final` schema

## Scheduling

Use the provided shell script with cron:

```bash
# Daily at midnight
0 0 * * * /path/to/pactravel-pipeline/run_pipeline.sh
```

## Alerting

Pipeline failures are logged to `logs/alerts.log` via Luigi's event handler system.
