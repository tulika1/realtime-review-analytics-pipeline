"""Kafka -> bronze -> silver -> enrich -> gold, every 15 minutes."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag

LAKE = "file:///opt/lake"


def spark_job(task_id: str, module: str, **kwargs) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"python -m reviewlens.spark.{module}",
                        **kwargs)


@dag(
    dag_id="reviewlens_pipeline",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                  "execution_timeout": timedelta(minutes=20)},
    tags=["reviewlens", "spark", "kafka"],
    doc_md=__doc__,
)
def reviewlens_pipeline():
    bronze = spark_job("kafka_to_bronze", "kafka_to_bronze",
                       outlets=[Asset(f"{LAKE}/bronze/reviews")])
    silver = spark_job("bronze_to_silver", "bronze_to_silver",
                       outlets=[Asset(f"{LAKE}/silver/reviews_current")])
    enrich = spark_job("enrich_reviews", "enrich",
                       outlets=[Asset(f"{LAKE}/silver/reviews_enriched")])
    # no retries: a failed quality check won't fix itself
    gold = spark_job("gold_write_audit_publish", "gold", retries=0,
                     outlets=[Asset(f"{LAKE}/gold/fct_review")])

    bronze >> silver >> enrich >> gold


reviewlens_pipeline()
