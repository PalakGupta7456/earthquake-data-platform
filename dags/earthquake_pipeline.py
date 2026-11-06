"""Apache Airflow DAG for the earthquake medallion pipeline.

    extract -> load_bronze -> load_silver -> dbt_build -> quality_gate -> analytics -> publish

Run it locally with `docker compose -f docker-compose.airflow.yml up` and open
http://localhost:8080 (user: airflow / password: airflow). The tasks call the
same functions the serverless GitHub Actions runner uses (src/quake/pipeline.py),
so both orchestrators always run identical logic.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="earthquake_medallion",
    description="USGS feed -> bronze (Parquet) -> silver (DuckDB) -> gold (dbt) -> analytics -> dashboard",
    schedule="17 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["data-engineering", "dbt", "duckdb", "usgs"],
)
def earthquake_medallion():
    @task
    def extract_and_land() -> dict:
        from quake import pipeline
        con = pipeline.connect()
        df = pipeline.extract(con)
        con.close()
        info = pipeline.load_bronze(df)
        return {"bronze_file": info["file"], "rows": info["rows"]}

    @task
    def load_silver(bronze: dict) -> dict:
        import pandas as pd

        from quake import pipeline
        from quake.config import DATA_DIR
        con = pipeline.connect()
        stats = pipeline.load_silver(con, pd.read_parquet(DATA_DIR / bronze["bronze_file"]))
        con.close()
        return stats

    @task
    def dbt_build(silver_stats: dict) -> dict:
        from quake import pipeline
        summary = pipeline.run_dbt()
        return {k: v for k, v in summary.items() if k != "results"}

    @task
    def quality_gate(dbt_summary: dict) -> dict:
        from quake import pipeline
        con = pipeline.connect()
        q = pipeline.quality(con)
        con.close()
        if not all(q["checks"].values()):
            raise AirflowFailException(f"Data quality gate failed: {q['checks']}")
        return q

    @task
    def analytics_and_publish(quality: dict, silver_stats: dict, dbt_summary: dict) -> dict:
        from quake import pipeline
        con = pipeline.connect()
        analysis = pipeline.analytics(con)
        out = pipeline.publish(con, {"silver": silver_stats, "dbt": dbt_summary, "quality": quality}, analysis)
        con.close()
        return out

    bronze = extract_and_land()
    silver = load_silver(bronze)
    dbt = dbt_build(silver)
    q = quality_gate(dbt)
    analytics_and_publish(q, silver, dbt)


earthquake_medallion()
