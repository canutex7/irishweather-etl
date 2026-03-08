"""pipeline/transform.py — Clean, validate, and enrich raw weather data."""

import pandas as pd
import logging

log = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["time", "temperature_2m", "windspeed_10m", "precipitation"]
ANOMALY_TEMP_HIGH = 35.0   # Celsius — unusual for Ireland
ANOMALY_TEMP_LOW  = -10.0  # Celsius — unusual for Ireland


def _feels_like(temp_c: float, windspeed_kmh: float) -> float:
    """
    Wind chill formula (Environment Canada / Australian BOM method).
    Valid for temps <= 10°C and wind >= 4.8 km/h.
    """
    if temp_c > 10 or windspeed_kmh < 4.8:
        return temp_c
    return (
        13.12
        + 0.6215 * temp_c
        - 11.37 * (windspeed_kmh ** 0.16)
        + 0.3965 * temp_c * (windspeed_kmh ** 0.16)
    )


def transform(raw: dict, city: str) -> pd.DataFrame:
    """
    Transform raw API response into a clean, analysis-ready DataFrame.

    Steps:
        1. Flatten hourly arrays into a DataFrame
        2. Validate required fields — drop rows with nulls
        3. Convert timestamp string → datetime
        4. Derive temp_f, feels_like_c, is_anomaly columns
        5. Add city column

    Args:
        raw:  Raw JSON dict from Open-Meteo API
        city: City name string

    Returns:
        Cleaned pd.DataFrame
    """
    hourly = raw.get("hourly", {})
    df = pd.DataFrame(hourly)

    # Validate columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns from API response: {missing}")

    # Drop rows with nulls in critical fields
    before = len(df)
    df.dropna(subset=["temperature_2m", "windspeed_10m"], inplace=True)
    dropped = before - len(df)
    if dropped:
        log.warning(f"{city}: Dropped {dropped} rows with null temperature/windspeed")

    # Parse timestamps
    df["timestamp"] = pd.to_datetime(df["time"])
    df.drop(columns=["time"], inplace=True)

    # Derived columns
    df["city"]         = city
    df["temp_c"]       = df["temperature_2m"].round(2)
    df["temp_f"]       = (df["temp_c"] * 9 / 5 + 32).round(2)
    df["windspeed"]    = df["windspeed_10m"].round(2)
    df["humidity"]     = df.get("relativehumidity_2m", pd.Series(dtype=float)).round(2)
    df["precipitation"] = df["precipitation"].fillna(0.0).round(2)
    df["feels_like_c"] = df.apply(
        lambda r: round(_feels_like(r["temp_c"], r["windspeed"]), 2), axis=1
    )
    df["is_anomaly"] = (
        (df["temp_c"] > ANOMALY_TEMP_HIGH) | (df["temp_c"] < ANOMALY_TEMP_LOW)
    ).astype(int)

    # Select and order final columns
    return df[[
        "city", "timestamp", "temp_c", "temp_f", "feels_like_c",
        "precipitation", "windspeed", "humidity", "is_anomaly"
    ]]
