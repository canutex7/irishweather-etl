"""pipeline/dashboard.py — Generate an HTML dashboard with weather charts."""

import sqlite3
import os
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend (no display needed)
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import base64
from io import BytesIO
from datetime import datetime

CITY_COLOURS = {
    "Dublin":    "#63b3ed",
    "Cork":      "#68d391",
    "Galway":    "#f6ad55",
    "Limerick":  "#fc8181",
    "Waterford": "#b794f4",
}


def _chart_to_base64(fig) -> str:
    """Convert a matplotlib figure to a base64 PNG string for embedding in HTML."""
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight",
                facecolor="#0d1424", edgecolor="none")
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return encoded


def _load_data(db_path: str) -> pd.DataFrame:
    """Load all weather readings from SQLite into a DataFrame."""
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM weather_readings ORDER BY timestamp ASC", conn
        )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def _make_temperature_chart(df: pd.DataFrame) -> str:
    """Line chart — temperature over time per city."""
    fig, ax = plt.subplots(figsize=(11, 4))
    fig.patch.set_facecolor("#0d1424")
    ax.set_facecolor("#080d1a")

    for city, colour in CITY_COLOURS.items():
        city_df = df[df["city"] == city]
        if city_df.empty:
            continue
        # Resample to 3-hour averages to keep chart readable
        city_df = city_df.set_index("timestamp").resample("3h")["temp_c"].mean().reset_index()
        ax.plot(city_df["timestamp"], city_df["temp_c"],
                label=city, color=colour, linewidth=1.8, alpha=0.9)

    ax.set_title("Temperature (°C) — Last 7 Days", color="#e2e8f0",
                 fontsize=13, pad=14, fontweight="bold")
    ax.set_ylabel("°C", color="#a0aec0", fontsize=11)
    ax.tick_params(colors="#718096", labelsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.grid(True, color="#1a2035", linewidth=0.8)
    ax.spines[:].set_color("#1a2035")
    ax.legend(loc="upper right", framealpha=0.15, labelcolor="#e2e8f0",
              fontsize=9, facecolor="#0d1424", edgecolor="#1a2035")

    fig.autofmt_xdate()
    return _chart_to_base64(fig)


def _make_precipitation_chart(df: pd.DataFrame) -> str:
    """Bar chart — total precipitation per city."""
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0d1424")
    ax.set_facecolor("#080d1a")

    cities = list(CITY_COLOURS.keys())
    totals = [df[df["city"] == c]["precipitation"].sum() for c in cities]
    colours = list(CITY_COLOURS.values())

    bars = ax.bar(cities, totals, color=colours, width=0.55, alpha=0.85)
    ax.bar_label(bars, fmt="%.1f mm", color="#a0aec0", fontsize=9, padding=4)

    ax.set_title("Total Precipitation (mm)", color="#e2e8f0",
                 fontsize=13, pad=14, fontweight="bold")
    ax.set_ylabel("mm", color="#a0aec0", fontsize=11)
    ax.tick_params(colors="#718096", labelsize=9)
    ax.grid(True, axis="y", color="#1a2035", linewidth=0.8)
    ax.spines[:].set_color("#1a2035")

    return _chart_to_base64(fig)


def _make_wind_chart(df: pd.DataFrame) -> str:
    """Line chart — average windspeed over time per city."""
    fig, ax = plt.subplots(figsize=(11, 4))
    fig.patch.set_facecolor("#0d1424")
    ax.set_facecolor("#080d1a")

    for city, colour in CITY_COLOURS.items():
        city_df = df[df["city"] == city]
        if city_df.empty:
            continue
        city_df = city_df.set_index("timestamp").resample("3h")["windspeed"].mean().reset_index()
        ax.plot(city_df["timestamp"], city_df["windspeed"],
                label=city, color=colour, linewidth=1.8, alpha=0.9)

    ax.set_title("Wind Speed (km/h) — Last 7 Days", color="#e2e8f0",
                 fontsize=13, pad=14, fontweight="bold")
    ax.set_ylabel("km/h", color="#a0aec0", fontsize=11)
    ax.tick_params(colors="#718096", labelsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.grid(True, color="#1a2035", linewidth=0.8)
    ax.spines[:].set_color("#1a2035")
    ax.legend(loc="upper right", framealpha=0.15, labelcolor="#e2e8f0",
              fontsize=9, facecolor="#0d1424", edgecolor="#1a2035")

    fig.autofmt_xdate()
    return _chart_to_base64(fig)


def _summary_stats(df: pd.DataFrame) -> list[dict]:
    """Compute per-city summary stats for the dashboard cards."""
    stats = []
    for city in CITY_COLOURS:
        city_df = df[df["city"] == city]
        if city_df.empty:
            continue
        stats.append({
            "city": city,
            "colour": CITY_COLOURS[city],
            "avg_temp": round(city_df["temp_c"].mean(), 1),
            "max_temp": round(city_df["temp_c"].max(), 1),
            "min_temp": round(city_df["temp_c"].min(), 1),
            "total_rain": round(city_df["precipitation"].sum(), 1),
            "avg_wind": round(city_df["windspeed"].mean(), 1),
            "anomalies": int(city_df["is_anomaly"].sum()),
            "records": len(city_df),
        })
    return stats


def generate_dashboard(db_path: str, report_path: str) -> None:
    """
    Generate a self-contained HTML dashboard from the weather database.

    Args:
        db_path:     Path to the SQLite database file.
        report_path: Output path for the HTML file.
    """
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    df = _load_data(db_path)
    if df.empty:
        print("No data in database yet — run the pipeline first.")
        return

    temp_chart   = _make_temperature_chart(df)
    precip_chart = _make_precipitation_chart(df)
    wind_chart   = _make_wind_chart(df)
    stats        = _summary_stats(df)
    generated_at = datetime.now().strftime("%d %b %Y at %H:%M")
    total_records = len(df)

    # Build stat cards HTML
    cards_html = ""
    for s in stats:
        cards_html += f"""
        <div class="card" style="border-top: 3px solid {s['colour']}">
          <div class="card-city">{s['city']}</div>
          <div class="card-row"><span>Avg Temp</span><strong>{s['avg_temp']}°C</strong></div>
          <div class="card-row"><span>Max / Min</span><strong>{s['max_temp']}°C / {s['min_temp']}°C</strong></div>
          <div class="card-row"><span>Total Rain</span><strong>{s['total_rain']} mm</strong></div>
          <div class="card-row"><span>Avg Wind</span><strong>{s['avg_wind']} km/h</strong></div>
          <div class="card-row"><span>Anomalies</span>
            <strong style="color:{'#fc8181' if s['anomalies'] > 0 else '#68d391'}">
              {s['anomalies']}
            </strong>
          </div>
          <div class="card-row"><span>Records</span><strong>{s['records']}</strong></div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IrishWeather ETL — Dashboard</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: #050810;
    color: #e2e8f0;
    padding: 40px 32px;
  }}
  header {{ margin-bottom: 40px; }}
  h1 {{ font-size: 28px; font-weight: 800; letter-spacing: -0.02em; }}
  h1 span {{ color: #63b3ed; }}
  .meta {{ color: #718096; font-size: 13px; margin-top: 6px; font-family: monospace; }}
  .meta strong {{ color: #a0aec0; }}

  .cards {{ display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 48px; }}
  .card {{
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 12px;
    padding: 20px;
    min-width: 160px;
    flex: 1;
  }}
  .card-city {{ font-weight: 700; font-size: 15px; margin-bottom: 14px; }}
  .card-row {{
    display: flex; justify-content: space-between; align-items: center;
    font-size: 12px; color: #718096; padding: 6px 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
  }}
  .card-row:last-child {{ border-bottom: none; }}
  .card-row strong {{ color: #e2e8f0; font-size: 13px; }}

  .chart-section {{ margin-bottom: 48px; }}
  .chart-label {{
    font-size: 11px; font-family: monospace; letter-spacing: 0.08em;
    color: #63b3ed; text-transform: uppercase; margin-bottom: 14px;
  }}
  .chart-section img {{ width: 100%; border-radius: 12px; border: 1px solid rgba(255,255,255,0.07); }}

  footer {{ color: #4a5568; font-size: 12px; font-family: monospace; margin-top: 40px; }}
</style>
</head>
<body>
<header>
  <h1>Irish<span>Weather</span> ETL Dashboard</h1>
  <p class="meta">Generated: <strong>{generated_at}</strong> &nbsp;·&nbsp; Total records: <strong>{total_records:,}</strong> &nbsp;·&nbsp; Cities: <strong>5</strong></p>
</header>

<div class="cards">{cards_html}</div>

<div class="chart-section">
  <div class="chart-label">// temperature trends</div>
  <img src="data:image/png;base64,{temp_chart}" alt="Temperature chart">
</div>

<div class="chart-section">
  <div class="chart-label">// wind speed trends</div>
  <img src="data:image/png;base64,{wind_chart}" alt="Wind speed chart">
</div>

<div class="chart-section" style="max-width: 600px;">
  <div class="chart-label">// total precipitation</div>
  <img src="data:image/png;base64,{precip_chart}" alt="Precipitation chart">
</div>

<footer>IrishWeather ETL Pipeline · Built by Canute Fernandes · github.com/canutex7</footer>
</body>
</html>"""

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard saved → {report_path}")
