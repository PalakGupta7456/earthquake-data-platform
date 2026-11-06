"""Paths and settings for the earthquake data platform."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"           # raw API pulls, partitioned by date (Parquet)
WAREHOUSE = DATA_DIR / "warehouse.duckdb"  # silver + gold (dbt) tables
DBT_DIR = ROOT / "dbt"
SITE_DATA_DIR = ROOT / "docs" / "data"

MIN_MAGNITUDE = 2.5
BACKFILL_DAYS = 365
BRONZE_KEEP_DAYS = 7

FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson"
FDSN_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
