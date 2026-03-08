"""
IrishWeather ETL Pipeline
Entry point — run once or on a schedule.
"""

import argparse
import logging
import schedule
import time

from pipeline.extract import extract_weather
from pipeline.transform import transform
from pipeline.load import init_db, load_to_db
from pipeline.dashboard import generate_dashboard
from config import CITIES, DB_PATH, REPORT_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ]
)
log = logging.getLogger(__name__)


def run_pipeline():
    """Full ETL cycle: extract → transform → load → report."""
    log.info("=== Pipeline run started ===")
    init_db(DB_PATH)

    total_rows = 0
    for city in CITIES:
        try:
            log.info(f"Extracting: {city}")
            raw = extract_weather(city)

            log.info(f"Transforming: {city}")
            df = transform(raw, city)

            log.info(f"Loading: {city} ({len(df)} rows)")
            rows = load_to_db(df, DB_PATH)
            total_rows += rows

        except Exception as e:
            log.error(f"Failed for {city}: {e}")

    log.info(f"Loaded {total_rows} total rows.")
    log.info("Generating dashboard...")
    generate_dashboard(DB_PATH, REPORT_PATH)
    log.info(f"Dashboard saved to {REPORT_PATH}")
    log.info("=== Pipeline run complete ===")


def main():
    parser = argparse.ArgumentParser(description="IrishWeather ETL Pipeline")
    parser.add_argument("--run-once", action="store_true", help="Run pipeline once and exit")
    parser.add_argument("--schedule", action="store_true", help="Run pipeline on hourly schedule")
    args = parser.parse_args()

    if args.run_once:
        run_pipeline()
    elif args.schedule:
        log.info("Scheduler started — pipeline runs every hour.")
        schedule.every().hour.do(run_pipeline)
        run_pipeline()  # Run immediately on start
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
