"""Stage 1: Kafka topic -> bronze Delta table (raw, append-only).

Structured Streaming with trigger(availableNow=True): each Airflow run reads every
offset since the last checkpoint, writes it, and stops. We get streaming's
exactly-once bookkeeping (offsets + Delta sink in one checkpoint) without paying
for an always-on consumer. Payloads are stored as raw strings: bronze never
rejects data, so a parsing bug can always be fixed by replaying bronze.

    python -m reviewlens.spark.kafka_to_bronze
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F

from reviewlens.spark.session import BRONZE, CHECKPOINTS, get_spark


def main() -> None:
    spark = get_spark("kafka_to_bronze", kafka=True)
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "reviews.v1"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)  # bounds memory on a big backlog
        .load()
    )
    bronze = source.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingested_at"),
        F.date_format(F.current_timestamp(), "yyyyMMdd").alias("ingest_date"),
    )
    query = (
        bronze.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/kafka_to_bronze")
        .partitionBy("ingest_date")
        .trigger(availableNow=True)
        .start(BRONZE)
    )
    query.awaitTermination()
    progress = query.lastProgress or {}
    print(f"bronze: ingested {progress.get('numInputRows', 0)} rows", flush=True)


if __name__ == "__main__":
    main()
