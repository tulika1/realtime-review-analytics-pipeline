"""AWS Glue 5 (Spark) job: bronze JSON -> silver Iceberg table `silver.reviews_current`.

Spark translation of reviewlens.transform.to_silver. The unit tests in tests/ cover
the semantics; this job is the scalable implementation of the same contract.

Job params: --bronze_path s3://.../bronze/reviews/ --ingest_date 20261008
            --warehouse s3://.../warehouse/ --quarantine_path s3://.../quarantine/
"""
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["bronze_path", "ingest_date", "warehouse", "quarantine_path"])
spark = (
    GlueContext(SparkContext.getOrCreate()).spark_session.builder
    .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue.warehouse", args["warehouse"])
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

raw = spark.read.json(f"{args['bronze_path']}ingest_date={args['ingest_date']}/")

# 1. Contract validation -> quarantine (never silently drop)
valid_cond = (
    F.col("event_id").isNotNull() & (F.col("review_id") != "") & F.col("review_id").isNotNull()
    & F.col("rating").between(1, 5) & F.col("op").isin("create", "update", "delete")
    & (F.col("schema_version") == "1") & (F.length("review_text") > 0)
    & F.to_timestamp("event_time").isNotNull()
)
raw = raw.withColumn("_valid", valid_cond)
raw.filter(~F.col("_valid")).drop("_valid").write.mode("append").json(
    f"{args['quarantine_path']}ingest_date={args['ingest_date']}/")
events = raw.filter("_valid").drop("_valid").withColumn("event_ts", F.to_timestamp("event_time"))

# 2. Dedup redelivered events, 3. latest-wins per review within the batch
events = events.dropDuplicates(["event_id"])
w = Window.partitionBy("review_id").orderBy(F.col("event_ts").desc(), F.col("event_id").desc())
latest = events.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# 5. PII redaction before anything downstream (incl. the LLM) sees the text
redacted = F.regexp_replace(
    F.regexp_replace("review_text", r"[\w.+-]+@[\w-]+\.[\w.-]+", "[EMAIL]"),
    r"\+?\d[\d\s().-]{7,}\d", "[PHONE]")
latest = (
    latest.withColumn("review_text_redacted", redacted)
    .withColumn("text_hash", F.substring(F.sha2("review_text_redacted", 256), 1, 16))
    .withColumn("is_deleted", F.col("op") == "delete")
    .withColumnRenamed("event_id", "last_event_id")
    .select("review_id", "product_id", "customer_id", "rating", "review_text_redacted",
            "text_hash", "event_ts", "is_deleted", "last_event_id")
)
latest.createOrReplaceTempView("batch")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue.silver.reviews_current (
  review_id string, product_id string, customer_id string, rating int,
  review_text_redacted string, text_hash string, event_ts timestamp,
  is_deleted boolean, last_event_id string, updated_at timestamp)
USING iceberg PARTITIONED BY (bucket(16, review_id))
TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read')
""")

# Idempotent across retries and overlapping backfills: only strictly newer events win
# (event_ts, then event_id as a deterministic tie-breaker).
spark.sql("""
MERGE INTO glue.silver.reviews_current t
USING batch s
ON t.review_id = s.review_id
WHEN MATCHED AND (s.event_ts > t.event_ts
                  OR (s.event_ts = t.event_ts AND s.last_event_id > t.last_event_id)) THEN
  UPDATE SET t.rating = s.rating,
             t.review_text_redacted = s.review_text_redacted,
             t.text_hash = s.text_hash,
             t.event_ts = s.event_ts,
             t.is_deleted = s.is_deleted,
             t.last_event_id = s.last_event_id,
             t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (review_id, product_id, customer_id, rating, review_text_redacted, text_hash,
          event_ts, is_deleted, last_event_id, updated_at)
  VALUES (s.review_id, s.product_id, s.customer_id, s.rating, s.review_text_redacted,
          s.text_hash, s.event_ts, s.is_deleted, s.last_event_id, current_timestamp())
""")
