"""Luigi Extract & Load tasks — one per source table."""

import logging
import luigi

from src.extract_load.db_connections import get_source_conn, get_dwh_conn

logger = logging.getLogger(__name__)

# Map: source table (public schema) → staging table (pactravel schema)
TABLES = [
    "aircrafts",
    "airlines",
    "airports",
    "customers",
    "hotel",
    "flight_bookings",
    "hotel_bookings",
]


def extract_load_table(table_name: str) -> None:
    """Truncate the staging table and bulk-copy all rows from source."""
    src_conn = get_source_conn()
    dwh_conn = get_dwh_conn()
    try:
        src_cur = src_conn.cursor()
        dwh_cur = dwh_conn.cursor()

        # Fetch column names and types from source
        src_cur.execute(
            """
            SELECT column_name, data_type, character_maximum_length,
                   is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (table_name,),
        )
        col_meta = src_cur.fetchall()
        columns = [row[0] for row in col_meta]
        col_list = ", ".join(f'"{c}"' for c in columns)

        logger.info("Extracting %s from source...", table_name)
        src_cur.execute(f'SELECT {col_list} FROM public."{table_name}";')
        rows = src_cur.fetchall()
        logger.info("Fetched %d rows from %s.", len(rows), table_name)

        # Build CREATE TABLE DDL from source column metadata
        col_defs = []
        for col_name, data_type, char_max_len, is_nullable, col_default in col_meta:
            if data_type == "character varying" and char_max_len:
                pg_type = f"varchar({char_max_len})"
            elif data_type == "character varying":
                pg_type = "varchar"
            else:
                pg_type = data_type
            nullable = "" if is_nullable == "YES" else " NOT NULL"
            col_defs.append(f'    "{col_name}" {pg_type}{nullable}')
        create_sql = (
            f'CREATE TABLE IF NOT EXISTS pactravel."{table_name}" (\n'
            + ",\n".join(col_defs)
            + "\n);"
        )
        dwh_cur.execute(create_sql)

        # Truncate staging table
        dwh_cur.execute(f'TRUNCATE TABLE pactravel."{table_name}";')

        # Bulk insert
        if rows:
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = (
                f'INSERT INTO pactravel."{table_name}" ({col_list}) '
                f"VALUES ({placeholders});"
            )
            # col_list already has quoted column names
            dwh_cur.executemany(insert_sql, rows)

        dwh_conn.commit()
        logger.info("Loaded %d rows into pactravel.%s.", len(rows), table_name)
    finally:
        src_conn.close()
        dwh_conn.close()


class ExtractLoadTable(luigi.Task):
    """Generic EL task for a single table."""

    table_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"temp/el_{self.table_name}.done")

    def run(self):
        extract_load_table(self.table_name)
        with self.output().open("w") as f:
            f.write("done")


class ExtractLoadAircrafts(ExtractLoadTable):
    table_name = "aircrafts"


class ExtractLoadAirlines(ExtractLoadTable):
    table_name = "airlines"


class ExtractLoadAirports(ExtractLoadTable):
    table_name = "airports"


class ExtractLoadCustomers(ExtractLoadTable):
    table_name = "customers"


class ExtractLoadHotel(ExtractLoadTable):
    table_name = "hotel"


class ExtractLoadFlightBookings(ExtractLoadTable):
    table_name = "flight_bookings"


class ExtractLoadHotelBookings(ExtractLoadTable):
    table_name = "hotel_bookings"


ALL_EL_TASKS = [
    ExtractLoadAircrafts,
    ExtractLoadAirlines,
    ExtractLoadAirports,
    ExtractLoadCustomers,
    ExtractLoadHotel,
    ExtractLoadFlightBookings,
    ExtractLoadHotelBookings,
]
