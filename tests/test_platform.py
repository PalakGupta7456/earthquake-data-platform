import numpy as np
import pandas as pd
import pytest

from quake import ingest, pipeline, silver
from quake.seismology import b_value, find_clusters, magnitude_of_completeness
from quake.synthetic import generate

END = pd.Timestamp("2026-03-01 00:00")


@pytest.fixture()
def bronze():
    return ingest.features_to_frame(generate(days=60, per_day=40, end=END), "test")


@pytest.fixture()
def con(tmp_path):
    c = pipeline.connect(tmp_path / "wh.duckdb")
    yield c
    c.close()


def test_region_parsing():
    assert silver.region_from_place("12 km SSW of Volcano, Hawaii") == "Hawaii"
    assert silver.region_from_place("Kermadec Islands region") == "Kermadec Islands"
    assert silver.region_from_place("5 km N of Ridgecrest, CA") == "California"
    assert silver.region_from_place(None) == "Unknown"


def test_bad_rows_are_quarantined(bronze):
    bad = bronze.head(3).copy()
    bad.loc[bad.index[0], "mag"] = 42
    bad.loc[bad.index[1], "lat"] = 123
    bad.loc[bad.index[2], "event_type"] = "quarry blast"
    good, q = silver.validate(bad)
    assert len(good) == 0 and len(q) == 3
    assert {"bad magnitude;", "bad coordinates;", "not an earthquake;"} == set(q["reason"])


def test_upsert_is_idempotent(con, bronze):
    first = silver.upsert(con, bronze)
    second = silver.upsert(con, bronze)
    assert first["inserted"] == first["valid"]
    assert second["inserted"] == 0 and second["updated"] == 0
    assert second["silver_rows"] == first["silver_rows"]


def test_newer_revision_wins_and_older_is_ignored(con, bronze):
    silver.upsert(con, bronze)
    eid = bronze["event_id"].iloc[0]
    newer = bronze.iloc[[0]].assign(mag=6.6, updated_ms=bronze["updated_ms"].iloc[0] + 1000)
    older = bronze.iloc[[0]].assign(mag=1.1, updated_ms=bronze["updated_ms"].iloc[0] - 1000)
    assert silver.upsert(con, newer)["updated"] == 1
    silver.upsert(con, older)
    assert con.execute("SELECT mag FROM silver_events WHERE event_id = ?", [eid]).fetchall()[0][0] == 6.6


def test_b_value_recovers_truth():
    rng = np.random.default_rng(0)
    mags = np.round(2.0 + rng.exponential(1 / (1.2 * np.log(10)), 20000), 1)  # true b = 1.2
    mc = magnitude_of_completeness(mags)
    b, se, n = b_value(mags, mc)
    assert abs(b - 1.2) < 3 * se + 0.05


def test_clusters_find_planted_sequences():
    events = ingest.features_to_frame(generate(days=60, per_day=40, end=END), "t")
    good, _ = silver.validate(events)
    clusters, _ = find_clusters(good.rename(columns={})[["event_id", "event_time", "lat", "lon", "mag", "place", "region"]])
    assert any(c["kind"] == "Aftershock sequence" and c["max_mag"] >= 6 for c in clusters)


def test_full_pipeline_with_dbt(tmp_path, monkeypatch, bronze):
    """End to end: bronze -> silver -> dbt build (models + tests) -> analytics -> publish."""
    wh = tmp_path / "wh.duckdb"
    con = pipeline.connect(wh)
    silver.upsert(con, bronze)
    con.close()
    summary = pipeline.run_dbt(warehouse=wh)
    assert summary["success"] and summary["tests"] >= 10 and summary["tests_passed"] == summary["tests"]
    con = pipeline.connect(wh)
    analysis = pipeline.analytics(con)
    out = pipeline.publish(con, {"test": True}, analysis, out_dir=tmp_path / "site")
    con.close()
    assert out["events_on_map"] > 0
    assert (tmp_path / "site" / "dashboard.json").exists()
