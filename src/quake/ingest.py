"""Bronze layer: land raw USGS events exactly as received, partitioned by date.

* First run (empty warehouse): backfill 365 days from the FDSN event API in
  monthly chunks.
* Every run after that: pull the rolling 7-day GeoJSON feed. USGS revises
  events after publication (magnitude, location, review status), so the same
  event arrives many times; the silver layer keeps only the latest revision.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime, timedelta

import pandas as pd
import requests

from .config import BACKFILL_DAYS, BRONZE_DIR, BRONZE_KEEP_DAYS, FDSN_URL, FEED_URL, MIN_MAGNITUDE

log = logging.getLogger(__name__)
HEADERS = {"User-Agent": "earthquake-data-platform (portfolio project)"}


def _get_json(url: str, params: dict | None = None, retries: int = 4) -> dict:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=90)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:  # pragma: no cover - network
            wait = 3 * 2**attempt
            log.warning("%s, retrying in %ss", exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url}")


def features_to_frame(features: list[dict], source: str) -> pd.DataFrame:
    """Flatten GeoJSON features. The untouched record is kept in `raw` for replay."""
    rows = []
    ingested = datetime.now(UTC)
    for f in features:
        p = f.get("properties", {})
        lon, lat, depth = (f.get("geometry") or {}).get("coordinates", [None, None, None])[:3]
        rows.append({
            "event_id": f.get("id"), "time_ms": p.get("time"), "updated_ms": p.get("updated"),
            "mag": p.get("mag"), "mag_type": p.get("magType"), "place": p.get("place"),
            "lat": lat, "lon": lon, "depth_km": depth, "sig": p.get("sig"), "tsunami": p.get("tsunami"),
            "alert": p.get("alert"), "status": p.get("status"), "felt": p.get("felt"), "event_type": p.get("type"),
            "url": p.get("url"), "source": source, "ingested_at": ingested, "raw": json.dumps(f, separators=(",", ":")),
        })
    return pd.DataFrame(rows)


def fetch_feed() -> pd.DataFrame:
    return features_to_frame(_get_json(FEED_URL)["features"], "feed_2.5_week")


def fetch_backfill(days: int = BACKFILL_DAYS) -> pd.DataFrame:
    end = datetime.now(UTC)
    frames, cursor = [], end - timedelta(days=days)
    while cursor < end:
        nxt = min(cursor + timedelta(days=30), end)
        payload = _get_json(FDSN_URL, {"format": "geojson", "starttime": cursor.isoformat(), "endtime": nxt.isoformat(),
                                       "minmagnitude": MIN_MAGNITUDE, "orderby": "time-asc", "limit": 20000})
        frames.append(features_to_frame(payload["features"], "fdsn_backfill"))
        log.info("backfill %s → %s: %d events", cursor.date(), nxt.date(), len(payload["features"]))
        cursor = nxt
        time.sleep(1)
    return pd.concat(frames, ignore_index=True)


def write_bronze(df: pd.DataFrame, bronze_dir=BRONZE_DIR) -> str:
    now = datetime.now(UTC)
    part = bronze_dir / f"dt={now:%Y-%m-%d}"
    part.mkdir(parents=True, exist_ok=True)
    path = part / f"{now:%H%M%S}_{df['source'].iloc[0] if len(df) else 'empty'}.parquet"
    df.to_parquet(path, index=False, compression="zstd")
    return str(path)


def prune_bronze(bronze_dir=BRONZE_DIR, keep_days: int = BRONZE_KEEP_DAYS, keep_files: int = 48) -> int:
    """Retention: drop partitions older than keep_days and cap the total number of
    files (the hourly feed overlaps itself, so older pulls are fully redundant)."""
    cutoff = (datetime.now(UTC) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    files = sorted(bronze_dir.glob("dt=*/*.parquet"), key=lambda f: (f.parent.name, f.name))
    removed = 0
    for i, f in enumerate(files):
        if f.parent.name[3:] < cutoff or i < len(files) - keep_files:
            f.unlink()
            removed += 1
    for part in bronze_dir.glob("dt=*"):
        if not any(part.iterdir()):
            part.rmdir()
    return removed
