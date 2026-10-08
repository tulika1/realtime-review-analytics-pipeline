"""Stage 4: gold marts with write-audit-publish.

1. WRITE   all gold tables to gold_audit/
2. AUDIT   run data-quality checks on the audit copies; record results in gold/_quality_runs
3. PUBLISH overwrite gold/ only if every blocking check passed (a Delta overwrite is
           atomic, so dashboard readers see either the old or the new version, never half)

If a blocking check fails the task exits non-zero: Airflow marks it failed and
alerts, and consumers keep the last good data.

    python -m reviewlens.spark.gold
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from reviewlens.enrich import PROMPT_VERSION
from reviewlens.spark.session import (
    BRONZE,
    ENRICHED,
    GOLD,
    GOLD_AUDIT,
    QUARANTINE,
    SILVER,
    get_spark,
)


@dataclass
class CheckResult:
    name: str
    value: float
    threshold: str
    passed: bool
    severity: str  # "block" or "warn"


def build_tables(silver: DataFrame, enriched: DataFrame) -> dict[str, DataFrame]:
    # Latest insight per (review, exact text) for the current prompt version.
    w = Window.partitionBy("review_id", "text_hash").orderBy(F.col("enriched_at").desc())
    current = (enriched.filter(F.col("prompt_version") == PROMPT_VERSION)
               .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn"))

    fct = (
        silver.filter("NOT is_deleted")  # tombstones disappear from every mart here
        .join(current, ["review_id", "text_hash"])  # never pair an insight with edited text
        .select("review_id", "product_id", "rating", "event_ts",
                F.to_date("event_ts").alias("review_date"),
                "review_text_redacted", "sentiment", "sentiment_score", "topics",
                "is_actionable", "summary", "model_id", "prompt_version")
    )
    health = fct.groupBy("product_id", "review_date").agg(
        F.count("*").alias("reviews"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(F.avg("sentiment_score"), 2).alias("avg_sentiment"),
        F.round(100 * F.avg(F.col("is_actionable").cast("int")), 1).alias("pct_actionable"),
    )
    topics = (fct.filter("is_actionable")
              .select("product_id", F.explode("topics").alias("topic"))
              .groupBy("product_id", "topic").agg(F.count("*").alias("actionable_mentions")))
    return {"fct_review": fct, "mart_product_health_daily": health, "mart_topic_issues": topics}


def run_checks(spark: SparkSession, audit: dict[str, DataFrame], live_count: int) -> list[CheckResult]:
    fct = audit["fct_review"]
    n = fct.count()
    results = []

    dupes = n - fct.select("review_id").distinct().count()
    results.append(CheckResult("fct_review_id_unique", dupes, "== 0", dupes == 0, "block"))

    coverage = n / live_count if live_count else 1.0
    results.append(CheckResult("enrichment_coverage", round(coverage, 4), ">= 0.95",
                               coverage >= 0.95, "block"))

    bad_sent = fct.filter(~F.col("sentiment").isin("positive", "neutral", "negative", "mixed")).count()
    results.append(CheckResult("sentiment_accepted_values", bad_sent, "== 0", bad_sent == 0, "block"))

    bronze_n = spark.read.format("delta").load(BRONZE).count()
    q_n = (spark.read.format("delta").load(QUARANTINE).count()
           if DeltaTable.isDeltaTable(spark, QUARANTINE) else 0)
    q_rate = q_n / bronze_n if bronze_n else 0.0
    results.append(CheckResult("contract_quarantine_rate", round(q_rate, 4), "<= 0.02",
                               q_rate <= 0.02, "block"))

    agree = fct.filter(
        ((F.col("sentiment") == "negative") & (F.col("rating") <= 2))
        | ((F.col("sentiment") == "positive") & (F.col("rating") >= 4))
        | F.col("sentiment").isin("mixed", "neutral")
    ).count()
    agreement = agree / n if n else 1.0
    results.append(CheckResult("sentiment_rating_agreement", round(agreement, 4), ">= 0.80",
                               agreement >= 0.80, "warn"))
    return results


def main() -> None:
    spark = get_spark("gold")
    if not DeltaTable.isDeltaTable(spark, ENRICHED):
        print("gold: no enriched data yet, nothing to publish", flush=True)
        return
    silver = spark.read.format("delta").load(SILVER)
    enriched = spark.read.format("delta").load(ENRICHED)
    live_count = silver.filter("NOT is_deleted").count()

    # 1. WRITE to audit
    for name, df in build_tables(silver, enriched).items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .save(f"{GOLD_AUDIT}/{name}")
    audit = {name: spark.read.format("delta").load(f"{GOLD_AUDIT}/{name}")
             for name in ["fct_review", "mart_product_health_daily", "mart_topic_issues"]}

    # 2. AUDIT
    checks = run_checks(spark, audit, live_count)
    publish = all(c.passed for c in checks if c.severity == "block")
    run_rows = [{**asdict(c), "value": float(c.value), "published": publish} for c in checks]
    (spark.createDataFrame(run_rows).withColumn("run_at", F.current_timestamp())
     .write.format("delta").mode("append").option("mergeSchema", "true")
     .save(f"{GOLD}/_quality_runs"))
    for c in checks:
        print(f"[{'PASS' if c.passed else 'FAIL'}] {c.name:<28} {c.value}  ({c.threshold}, "
              f"{c.severity})", flush=True)

    if not publish:
        raise SystemExit("quality gate failed: gold NOT published; consumers keep last good data")

    # 3. PUBLISH (atomic overwrite per table)
    for name, df in audit.items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .save(f"{GOLD}/{name}")
    print("gold: published", flush=True)


if __name__ == "__main__":
    main()
