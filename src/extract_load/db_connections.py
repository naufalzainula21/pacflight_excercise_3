"""Database connection helpers for Source DB and DWH DB."""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_source_conn():
    """Return a psycopg2 connection to the Source DB."""
    return psycopg2.connect(
        host=os.getenv("SRC_POSTGRES_HOST"),
        port=int(os.getenv("SRC_POSTGRES_PORT")),
        dbname=os.getenv("SRC_POSTGRES_DB"),
        user=os.getenv("SRC_POSTGRES_USER"),
        password=os.getenv("SRC_POSTGRES_PASSWORD"),
    )


def get_dwh_conn():
    """Return a psycopg2 connection to the DWH DB."""
    return psycopg2.connect(
        host=os.getenv("DWH_POSTGRES_HOST"),
        port=int(os.getenv("DWH_POSTGRES_PORT")),
        dbname=os.getenv("DWH_POSTGRES_DB"),
        user=os.getenv("DWH_POSTGRES_USER"),
        password=os.getenv("DWH_POSTGRES_PASSWORD"),
    )
