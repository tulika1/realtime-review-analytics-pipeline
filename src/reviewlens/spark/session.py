"""Shared Spark session and lake paths."""
from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

LAKE = os.environ.get("LAKE_PATH", "/opt/lake")
BRONZE = f"{LAKE}/bronze/reviews"
SILVER = f"{LAKE}/silver/reviews_current"
ENRICHED = f"{LAKE}/silver/reviews_enriched"
QUARANTINE = f"{LAKE}/quarantine/reviews"
DLQ = f"{LAKE}/dlq/enrichment"
GOLD = f"{LAKE}/gold"
GOLD_AUDIT = f"{LAKE}/gold_audit"
CHECKPOINTS = f"{LAKE}/_checkpoints"


def get_spark(app: str, kafka: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", "4")  # default 200 is far too many locally
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    extra = [KAFKA_PACKAGE] if kafka else []
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
