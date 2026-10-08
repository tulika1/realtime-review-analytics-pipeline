"""Spark-stage tests. Run in CI (Linux) or inside the Airflow container:

    docker compose exec airflow python -m pytest /opt/reviewlens/tests/spark -q

Skipped on plain Windows (Spark needs Hadoop's winutils there) or without pyspark.
"""
import json
import random
import sys

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")
if sys.platform == "win32":
    pytest.skip("Spark tests run on Linux / in Docker", allow_module_level=True)

from pyspark.sql import functions as F

from reviewlens.generator import generate
from reviewlens.spark.bronze_to_silver import process_batch
from reviewlens.spark.session import get_spark
from reviewlens.transform import to_silver


@pytest.fixture(scope="module")
def spark():
    s = get_spark("tests")
    yield s
    s.stop()


def bronze_df(spark, events):
    rows = [(e["review_id"], json.dumps(e)) for e in events]
    return (spark.createDataFrame(rows, "kafka_key string, payload string")
            .withColumn("kafka_ts", F.current_timestamp())
            .withColumn("ingested_at", F.current_timestamp()))


def silver_state(spark, path):
    return {r["review_id"]: (r["rating"], r["is_deleted"], r["text_hash"])
            for r in spark.read.format("delta").load(path).collect()}


def test_spark_silver_matches_python_reference(spark, tmp_path):
    events = generate(150)
    silver, quarantine = str(tmp_path / "silver"), str(tmp_path / "q")
    process_batch(bronze_df(spark, events), 0, silver, quarantine)

    expected = {r["review_id"]: (r["rating"], r["is_deleted"], r["text_hash"])
                for r in to_silver(events).rows}
    assert silver_state(spark, silver) == expected
    assert spark.read.format("delta").load(quarantine).count() == 2


def test_spark_merge_is_idempotent_and_order_independent(spark, tmp_path):
    events = generate(150)
    shuffled = events[:]
    random.Random(3).shuffle(shuffled)
    a, b = str(tmp_path / "a"), str(tmp_path / "b")
    q = str(tmp_path / "q")

    process_batch(bronze_df(spark, events), 0, a, q)
    process_batch(bronze_df(spark, events), 1, a, q)  # full replay
    # Same events split into two batches, second half first.
    half = len(shuffled) // 2
    process_batch(bronze_df(spark, shuffled[half:]), 0, b, q)
    process_batch(bronze_df(spark, shuffled[:half]), 1, b, q)

    assert silver_state(spark, a) == silver_state(spark, b)


def test_malformed_payload_is_quarantined_not_dropped(spark, tmp_path):
    batch = spark.createDataFrame([("k", "{not json")], "kafka_key string, payload string") \
        .withColumn("kafka_ts", F.current_timestamp()).withColumn("ingested_at", F.current_timestamp())
    q = str(tmp_path / "q")
    process_batch(batch, 0, str(tmp_path / "s"), q)
    row = spark.read.format("delta").load(q).first()
    assert "malformed_json" in row["errors"]
