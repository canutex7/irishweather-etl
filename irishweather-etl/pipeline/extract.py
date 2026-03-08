"""pipeline/extract.py — Fetch raw weather data from Open-Meteo API."""

import requests
from config import BASE_URL, CITIES


def extract_weather(city: str) -> dict:
    """
    Fetch hourly weather forecast for a given Irish city.

    Args:
        city: City name (must exist in config.CITIES)

    Returns:
        Raw JSON response as a dict

    Raises:
        ValueError: If city is not configured
        requests.HTTPError: On non-200 API response
    """
    if city not in CITIES:
        raise ValueError(f"Unknown city: {city}. Valid: {list(CITIES.keys())}")

    params = {
        "latitude": CITIES[city]["lat"],
        "longitude": CITIES[city]["lon"],
        "hourly": "temperature_2m,precipitation,windspeed_10m,relativehumidity_2m",
        "timezone": "Europe/Dublin",
        "forecast_days": 7,
    }

    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()
