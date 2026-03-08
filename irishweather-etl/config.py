"""config.py — Cities, paths, and settings."""

CITIES = {
    "Dublin":    {"lat": 53.3498, "lon": -6.2603},
    "Cork":      {"lat": 51.8985, "lon": -8.4756},
    "Galway":    {"lat": 53.2707, "lon": -9.0568},
    "Limerick":  {"lat": 52.6638, "lon": -8.6267},
    "Waterford": {"lat": 52.2593, "lon": -7.1101},
}

BASE_URL = "https://api.open-meteo.com/v1/forecast"
DB_PATH = "db/weather.db"
REPORT_PATH = "reports/dashboard.html"
SCHEDULE_INTERVAL_HOURS = 1