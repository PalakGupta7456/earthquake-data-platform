"""Silver layer: typed, validated, de-duplicated events in DuckDB.

The upsert is idempotent: replaying the same bronze file twice changes
nothing, and an older revision can never overwrite a newer one.
"""
from __future__ import annotations

import duckdb
import pandas as pd

DDL = """
CREATE TABLE IF NOT EXISTS silver_events (
    event_id     VARCHAR PRIMARY KEY,
    event_time   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL,
    mag          DOUBLE NOT NULL,
    mag_type     VARCHAR,
    place        VARCHAR,
    region       VARCHAR,
    lat          DOUBLE NOT NULL,
    lon          DOUBLE NOT NULL,
    depth_km     DOUBLE,
    sig          INTEGER,
    tsunami      BOOLEAN,
    alert        VARCHAR,
    status       VARCHAR,
    felt         INTEGER,
    url          VARCHAR,
    source       VARCHAR,
    ingested_at  TIMESTAMP
);
CREATE TABLE IF NOT EXISTS quarantine_events (
    event_id VARCHAR, reason VARCHAR, raw VARCHAR, ingested_at TIMESTAMP
);
"""


def region_from_place(place: str | None) -> str:
    """USGS places look like '12 km SSW of Volcano, Hawaii' or 'Kermadec Islands region'."""
    if not place:
        return "Unknown"
    tail = place.split(",")[-1].strip() if "," in place else place.split(" of ")[-1].strip()
    tail = tail.removesuffix(" region").strip()
    aliases = {"CA": "California", "Hawaii": "Hawaii", "B.C.": "Mexico", "MX": "Mexico", "AK": "Alaska", "NV": "Nevada"}
    return aliases.get(tail, tail) or "Unknown"


def validate(bronze: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split into valid rows and quarantined rows (with a reason)."""
    df = bronze.copy()
    reasons = pd.Series("", index=df.index)
    reasons[df["event_id"].isna()] += "missing id;"
    reasons[df["mag"].isna() | ~df["mag"].between(-2, 10)] += "bad magnitude;"
    reasons[~df["lat"].between(-90, 90) | ~df["lon"].between(-180, 180)] += "bad coordinates;"
    reasons[df["time_ms"].isna()] += "missing time;"
    reasons[df["event_type"].fillna("earthquake") != "earthquake"] += "not an earthquake;"
    bad = reasons != ""
    quarantine = df[bad].assign(reason=reasons[bad])[["event_id", "reason", "raw", "ingested_at"]]
    good = df[~bad].copy()
    good["event_time"] = pd.to_datetime(good["time_ms"], unit="ms")
    good["updated_at"] = pd.to_datetime(good["updated_ms"].fillna(good["time_ms"]), unit="ms")
    good["region"] = good["place"].map(region_from_place)
    good["tsunami"] = good["tsunami"].fillna(0).astype(bool)
    # Keep only the newest revision of each event inside this batch
    good = good.sort_values("updated_at").drop_duplicates("event_id", keep="last")
    return good, quarantine


COLUMNS = ["event_id", "event_time", "updated_at", "mag", "mag_type", "place", "region", "lat", "lon", "depth_km",
           "sig", "tsunami", "alert", "status", "felt", "url", "source", "ingested_at"]


def upsert(con: duckdb.DuckDBPyConnection, bronze: pd.DataFrame) -> dict:
    [con.execute(stmt).fetchall() for stmt in DDL.split(";") if stmt.strip()]
    good, quarantine = validate(bronze)
    before = con.execute("SELECT count(*) FROM silver_events").fetchall()[0][0]
    staged = good[COLUMNS].copy()  # noqa: F841 (referenced by name in SQL)
    con.register("staged", staged)
    changed = con.execute("""
        SELECT count(*) FROM staged s LEFT JOIN silver_events e USING (event_id)
        WHERE e.event_id IS NULL OR s.updated_at > e.updated_at
    """).fetchall()[0][0]
    con.execute(f"""
        INSERT INTO silver_events SELECT {", ".join(COLUMNS)} FROM staged
        ON CONFLICT (event_id) DO UPDATE SET
            {", ".join(f"{c} = excluded.{c}" for c in COLUMNS if c != "event_id")}
        WHERE excluded.updated_at > silver_events.updated_at
    """).fetchall()
    con.unregister("staged")
    if len(quarantine):
        con.register("q", quarantine)
        con.execute("INSERT INTO quarantine_events SELECT * FROM q").fetchall()
        con.unregister("q")
    after = con.execute("SELECT count(*) FROM silver_events").fetchall()[0][0]
    return {"received": int(len(bronze)), "valid": int(len(good)), "quarantined": int(len(quarantine)),
            "inserted": int(after - before), "updated": int(changed - (after - before)), "silver_rows": int(after)}
