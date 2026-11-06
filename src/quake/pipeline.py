"""The pipeline's tasks. Each function is one task, used both by the Airflow DAG
(dags/earthquake_pipeline.py) and by the serverless GitHub Actions runner
(`python -m quake.pipeline`)."""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime

import duckdb
import pandas as pd

from . import ingest, silver
from .config import BRONZE_DIR, DBT_DIR, SITE_DATA_DIR, WAREHOUSE
from .seismology import find_clusters, gutenberg_richter

log = logging.getLogger("quake")


def connect(path=WAREHOUSE) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    [con.execute(stmt).fetchall() for stmt in silver.DDL.split(";") if stmt.strip()]
    con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR, task VARCHAR, started_at TIMESTAMP, seconds DOUBLE, status VARCHAR, detail VARCHAR)""").fetchall()
    return con


def _record(con, run_id: str, task: str, t0: float, status: str, detail: dict | str) -> None:
    con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?)",
                [run_id, task, datetime.fromtimestamp(t0, UTC).replace(tzinfo=None), round(time.time() - t0, 2), status,
                 json.dumps(detail, default=str) if not isinstance(detail, str) else detail]).fetchall()


# ---------------- tasks ----------------
def extract(con, synthetic: bool = False) -> pd.DataFrame:
    """Bronze: backfill a year on the first run, then pull the rolling 7-day feed."""
    if synthetic:
        from .synthetic import generate
        return ingest.features_to_frame(generate(), "synthetic")
    empty = con.execute("SELECT count(*) FROM silver_events").fetchall()[0][0] == 0
    return ingest.fetch_backfill() if empty else ingest.fetch_feed()


def load_bronze(df: pd.DataFrame, bronze_dir=BRONZE_DIR) -> dict:
    path = ingest.write_bronze(df, bronze_dir)
    pruned = ingest.prune_bronze(bronze_dir)
    return {"file": os.path.relpath(path, bronze_dir.parent), "rows": int(len(df)), "files_pruned": pruned}


def load_silver(con, df: pd.DataFrame) -> dict:
    return silver.upsert(con, df)


def run_dbt(warehouse=WAREHOUSE, dbt_dir=DBT_DIR) -> dict:
    """dbt build = run every model + every data test, in dependency order."""
    from dbt.cli.main import dbtRunner

    os.environ["QUAKE_WAREHOUSE"] = str(warehouse)
    args = ["build", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir), "--quiet"]
    res = dbtRunner().invoke(args)
    results = [{"node": r.node.name, "type": r.node.resource_type, "status": str(r.status), "seconds": round(r.execution_time, 2)}
               for r in (res.result or [])]
    summary = {
        "success": bool(res.success),
        "models": sum(r["type"] == "model" for r in results),
        "tests": sum(r["type"] == "test" for r in results),
        "tests_passed": sum(r["type"] == "test" and r["status"] == "pass" for r in results),
        "results": results,
    }
    if not res.success:
        raise RuntimeError(f"dbt build failed: {[r for r in results if r['status'] not in ('success', 'pass')]}")
    return summary


def quality(con) -> dict:
    q = con.execute("""
        SELECT count(*) AS rows,
               count(*) - count(DISTINCT event_id) AS duplicate_ids,
               max(event_time) AS latest_event,
               max(ingested_at) AS latest_ingest,
               avg(CASE WHEN depth_km IS NULL THEN 1 ELSE 0 END) AS null_depth_rate,
               avg(CASE WHEN status = 'reviewed' THEN 1 ELSE 0 END) AS reviewed_rate
        FROM silver_events""").fetchdf().iloc[0]
    quarantined = con.execute("SELECT count(*) FROM quarantine_events").fetchall()[0][0]
    lag_min = (pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timestamp(q["latest_event"])).total_seconds() / 60
    return {
        "rows": int(q["rows"]), "duplicate_ids": int(q["duplicate_ids"]), "quarantined": int(quarantined),
        "latest_event": str(q["latest_event"]), "freshness_minutes": round(lag_min, 1),
        "null_depth_rate": round(float(q["null_depth_rate"]), 4), "reviewed_rate": round(float(q["reviewed_rate"]), 4),
        "checks": {"no_duplicates": int(q["duplicate_ids"]) == 0, "fresh_under_3h": lag_min < 180},
    }


def analytics(con) -> dict:
    events = con.execute("SELECT * FROM stg_events").fetchdf()
    fits = gutenberg_richter(events)
    clusters, labels = find_clusters(events)
    return {"gutenberg_richter": fits, "clusters": clusters, "cluster_labels": labels.to_dict()}


def publish(con, run: dict, analysis: dict, out_dir=SITE_DATA_DIR) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    recent = con.execute("SELECT * FROM fct_recent_events").fetchdf()
    labels = analysis["cluster_labels"]
    daily = con.execute("SELECT * FROM fct_daily_activity ORDER BY event_date DESC LIMIT 120").fetchdf().sort_values("event_date")
    regions = con.execute("SELECT * FROM mart_region_activity LIMIT 15").fetchdf()
    runs = con.execute("""SELECT run_id, min(started_at) AS started, sum(seconds) AS seconds,
                          bool_and(status = 'ok') AS ok FROM pipeline_runs GROUP BY run_id ORDER BY started DESC LIMIT 24""").fetchdf()
    payload = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "run": run,
        "events": [{"id": r.event_id, "t": r.event_time.isoformat(), "m": round(float(r.mag), 1), "p": r.place, "lat": round(float(r.lat), 3),
                    "lon": round(float(r.lon), 3), "d": None if pd.isna(r.depth_km) else round(float(r.depth_km), 1),
                    "c": int(labels.get(r.event_id, -1)), "u": r.url} for r in recent.itertuples()],
        "daily": {"date": [str(d)[:10] for d in daily["event_date"]], "events": daily["events"].astype(int).tolist(),
                  "m45": daily["events_m45"].astype(int).tolist(), "avg7": daily["events_7d_avg"].round(1).tolist(),
                  "energy": daily["energy_joules"].tolist()},
        "regions": json.loads(regions.to_json(orient="records", date_format="iso")),
        "gutenberg_richter": analysis["gutenberg_richter"],
        "clusters": [{k: v for k, v in c.items() if k != "event_ids"} for c in analysis["clusters"][:10]],
        "runs": json.loads(runs.to_json(orient="records", date_format="iso")),
    }
    (out_dir / "dashboard.json").write_text(json.dumps(payload, separators=(",", ":"), default=str))
    return {"events_on_map": len(payload["events"]), "clusters": len(payload["clusters"])}


# ---------------- serverless runner ----------------
def run(synthetic: bool = False) -> dict:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    con = connect()
    report: dict = {"run_id": run_id}

    def step(name, fn, *a):
        t0 = time.time()
        try:
            out = fn(*a)
        except Exception as exc:
            _record(con, run_id, name, t0, "failed", str(exc))
            raise
        detail = out if not isinstance(out, pd.DataFrame) else {"rows": len(out)}
        _record(con, run_id, name, t0, "ok", detail if name != "analytics" else {"clusters": len(out["clusters"])})
        log.info("%-12s %.1fs", name, time.time() - t0)
        return out

    bronze = step("extract", extract, con, synthetic)
    report["bronze"] = step("load_bronze", load_bronze, bronze)
    report["silver"] = step("load_silver", load_silver, con, bronze)
    con.close()  # dbt opens the warehouse itself
    dbt_summary = step_dbt(run_id, report)
    con = connect()
    report["quality"] = step("quality", quality, con)
    analysis = step("analytics", analytics, con)
    report["dbt"] = {k: v for k, v in dbt_summary.items() if k != "results"}
    report["published"] = step("publish", publish, con, report, analysis)
    con.close()
    return report


def step_dbt(run_id: str, report: dict) -> dict:
    t0 = time.time()
    summary = run_dbt()
    con = connect()
    _record(con, run_id, "dbt_build", t0, "ok", {k: v for k, v in summary.items() if k != "results"})
    con.close()
    log.info("%-12s %.1fs (%d models, %d/%d tests)", "dbt_build", time.time() - t0, summary["models"], summary["tests_passed"], summary["tests"])
    return summary


def main() -> None:
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--synthetic", action="store_true", help="use generated data instead of the USGS API")
    report = run(synthetic=ap.parse_args().synthetic)
    print(json.dumps({k: v for k, v in report.items() if k != "run"}, indent=2, default=str))


if __name__ == "__main__":
    main()
