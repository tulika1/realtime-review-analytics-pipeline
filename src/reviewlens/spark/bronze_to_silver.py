"""Stage 2: bronze -> silver `reviews_current` (one row per review, SCD1).

Spark version of reviewlens.transform.to_silver (same rules, tested both ways):
  1. parse + validate against the ReviewEvent v1 contract -> quarantine with reasons
  2. drop redelivered event_ids
  3. latest event per review_id by (event_time, event_id)
  4. deletes become tombstones
  5. PII redaction + text_hash before anything downstream sees the text
  6. MERGE that only accepts strictly newer events -> idempotent and order-independent

Reads bronze incrementally (Delta streaming source + availableNow), so each run
only touches new rows, and a crashed run resumes from its checkpoint.

    python -m reviewlens.spark.bronze_to_silver
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from reviewlens.spark.session import BRONZE, CHECKPOINTS, QUARANTINE, SILVER, get_spark

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("review_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("rating", T.IntegerType()),
    T.StructField("review_text", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("op", T.StringType()),
    T.StructField("schema_version", T.StringType()),
])
EMAIL_RE = r"[\w.+-]+@[\w-]+\.[\w.-]+"
PHONE_RE = r"\+?\d[\d\s().-]{7,}\d"


def _bad(cond) -> F.Column:
    """True when cond is false OR null (null-safe 'this check failed')."""
    return ~F.coalesce(cond, F.lit(False))


def parse_and_validate(bronze: DataFrame) -> DataFrame:
    """Adds `e` (parsed event), `event_ts` and `errors` (array; empty = valid)."""
    df = bronze.withColumn("e", F.from_json("payload", EVENT_SCHEMA))
    df = df.withColumn("event_ts", F.try_to_timestamp(F.col("e.event_time")))
    checks = [(F.col("e").isNotNull(), "malformed_json")]
    for field in ["event_id", "review_id", "product_id", "customer_id", "review_text"]:
        checks.append((F.length(F.col(f"e.{field}")) > 0, f"missing:{field}"))
    checks += [
        (F.col("e.rating").between(1, 5), "invalid:rating"),
        (F.col("e.op").isin("create", "update", "delete"), "invalid:op"),
        (F.col("e.schema_version") == "1", "invalid:schema_version"),
        (F.col("event_ts").isNotNull(), "invalid:event_time"),
        (F.length("e.review_text") <= 10_000, "invalid:review_text"),
    ]
    errors = F.array(*[F.when(_bad(c), F.lit(name)) for c, name in checks])
    return df.withColumn("errors", F.filter(errors, lambda x: x.isNotNull()))


def latest_per_review(valid: DataFrame) -> DataFrame:
    w = Window.partitionBy("review_id").orderBy(F.col("event_ts").desc(), F.col("event_id").desc())
    redacted = F.regexp_replace(F.regexp_replace("review_text", EMAIL_RE, "[EMAIL]"),
                                PHONE_RE, "[PHONE]")
    return (
        valid.select("e.*", "event_ts")
        .dropDuplicates(["event_id"])
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .withColumn("review_text_redacted", redacted)
        .withColumn("text_hash", F.substring(F.sha2("review_text_redacted", 256), 1, 16))
        .withColumn("is_deleted", F.col("op") == "delete")
        .withColumn("updated_at", F.current_timestamp())
        .select("review_id", "product_id", "customer_id", "rating", "review_text_redacted",
                "text_hash", "event_ts", "is_deleted",
                F.col("event_id").alias("last_event_id"), "updated_at")
    )


def merge_into_silver(spark: SparkSession, latest: DataFrame, silver_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, silver_path):
        latest.limit(0).write.format("delta").save(silver_path)
    (
        DeltaTable.forPath(spark, silver_path).alias("t")
        .merge(latest.alias("s"), "t.review_id = s.review_id")
        .whenMatchedUpdateAll(
            condition="s.event_ts > t.event_ts OR "
                      "(s.event_ts = t.event_ts AND s.last_event_id > t.last_event_id)")
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(batch: DataFrame, batch_id: int, silver_path: str = SILVER,
                  quarantine_path: str = QUARANTINE) -> None:
    spark = batch.sparkSession
    parsed = parse_and_validate(batch).persist()
    bad = parsed.filter(F.size("errors") > 0)
    if not bad.isEmpty():
        (bad.select("payload", "errors", "kafka_ts", "ingested_at",
                    F.lit(batch_id).alias("batch_id"))
         .write.format("delta").mode("append")
         # Idempotent append: a retried micro-batch is not written twice.
         .option("txnAppId", "bronze_to_silver_quarantine").option("txnVersion", batch_id)
         .save(quarantine_path))
    valid = parsed.filter(F.size("errors") == 0)
    if not valid.isEmpty():
        merge_into_silver(spark, latest_per_review(valid), silver_path)
    parsed.unpersist()


def main() -> None:
    spark = get_spark("bronze_to_silver")
    if not DeltaTable.isDeltaTable(spark, BRONZE):
        print("silver: bronze is empty, nothing to do", flush=True)
        return
    query = (
        spark.readStream.format("delta").load(BRONZE)
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_to_silver")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print(f"silver: processed {(query.lastProgress or {}).get('numInputRows', 0)} bronze rows",
          flush=True)


if __name__ == "__main__":
    main()
