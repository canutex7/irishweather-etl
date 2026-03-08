"""pipeline/load.py — Initialise SQLite DB and upsert weather records."""

import sqlite3
import logging
import os
import pandas as pd

log = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_readings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    city          TEXT    NOT NULL,
    timestamp     TEXT    NOT NULL,
    temp_c        REAL,
    temp_f        REAL,
    feels_like_c  REAL,
    precipitation REAL,
    windspeed     REAL,
    humidity      REAL,
    is_anomaly    INTEGER DEFAULT 0,
    ingested_at   TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_city_time ON weather_readings(city, timestamp);
"""

UPSERT_SQL = """
INSERT OR REPLACE INTO weather_readings
    (city, timestamp, temp_c, temp_f, feels_like_c, precipitation, windspeed, humidity, is_anomaly)
VALUES
    (:city, :timestamp, :temp_c, :temp_f, :feels_like_c, :precipitation, :windspeed, :humidity, :is_anomaly);
"""


def init_db(db_path: str) -> None:
    """Create the database file and schema if they don't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(CREATE_TABLE_SQL)
    log.info(f"Database ready: {db_path}")


def load_to_db(df: pd.DataFrame, db_path: str) -> int:
    """
    Upsert rows from DataFrame into the weather_readings table.

    Returns:
        Number of rows written.
    """
    df["timestamp"] = df["timestamp"].astype(str)
    records = df.to_dict(orient="records")

    with sqlite3.connect(db_path) as conn:
        cursor = conn.executemany(UPSERT_SQL, records)
        conn.commit()

    log.info(f"Upserted {cursor.rowcount} rows for {df['city'].iloc[0]}")
    return cursor.rowcount
