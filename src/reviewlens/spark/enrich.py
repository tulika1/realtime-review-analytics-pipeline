"""Stage 3: enrich new/changed live reviews (sentiment, topics, actionable, summary).

Only rows whose (review_id, text_hash) has no insight for the current
PROMPT_VERSION + model are processed (left-anti join), so re-runs, retries and
rating-only edits cost nothing. The enricher runs inside mapInPandas: rules by
default, a free local LLM with ENRICHER=ollama.

    python -m reviewlens.spark.enrich
"""
from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

from reviewlens.enrich import PROMPT_VERSION, enrich_one, make_enricher
from reviewlens.spark.session import DLQ, ENRICHED, SILVER, get_spark

OUT_SCHEMA = T.StructType([
    T.StructField("review_id", T.StringType()),
    T.StructField("text_hash", T.StringType()),
    T.StructField("model_id", T.StringType()),
    T.StructField("prompt_version", T.StringType()),
    T.StructField("sentiment", T.StringType()),
    T.StructField("sentiment_score", T.DoubleType()),
    T.StructField("topics", T.ArrayType(T.StringType())),
    T.StructField("is_actionable", T.BooleanType()),
    T.StructField("summary", T.StringType()),
    T.StructField("error", T.StringType()),
])
MAX_FAILURE_RATE = 0.05


def _enrich_partition(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    enricher = make_enricher()  # one client per partition, not per row
    for pdf in batches:
        out = []
        for row in pdf.to_dict("records"):
            outcome = enrich_one(row, enricher, max_attempts=3)
            base = {"review_id": row["review_id"], "text_hash": row["text_hash"],
                    "model_id": enricher.model_id, "prompt_version": PROMPT_VERSION}
            if outcome.insight:
                out.append({**base, **outcome.insight.model_dump(), "error": None})
            else:
                out.append({**base, "sentiment": None, "sentiment_score": None, "topics": None,
                            "is_actionable": None, "summary": None, "error": outcome.error})
        yield pd.DataFrame(out, columns=OUT_SCHEMA.fieldNames())


def main() -> None:
    spark = get_spark("enrich")
    if not DeltaTable.isDeltaTable(spark, SILVER):
        print("enrich: silver is empty, nothing to do", flush=True)
        return
    model_id = make_enricher().model_id
    live = spark.read.format("delta").load(SILVER).filter("NOT is_deleted")

    if DeltaTable.isDeltaTable(spark, ENRICHED):
        done = (spark.read.format("delta").load(ENRICHED)
                .filter((F.col("prompt_version") == PROMPT_VERSION) & (F.col("model_id") == model_id))
                .select("review_id", "text_hash"))
        todo = live.join(done, ["review_id", "text_hash"], "left_anti")
    else:
        todo = live

    todo = todo.select("review_id", "text_hash", "review_text_redacted", "rating")
    results = todo.mapInPandas(_enrich_partition, OUT_SCHEMA).persist()
    total = results.count()
    ok = results.filter("error IS NULL").drop("error").withColumn("enriched_at", F.current_timestamp())
    failed = results.filter("error IS NOT NULL").withColumn("failed_at", F.current_timestamp())

    # Keep paid-for / computed work even if the breaker trips below.
    ok.write.format("delta").mode("append").save(ENRICHED)
    n_failed = failed.count()
    if n_failed:
        failed.select("review_id", "text_hash", "model_id", "prompt_version", "error", "failed_at") \
            .write.format("delta").mode("append").save(DLQ)
    results.unpersist()

    print(f"enrich: {total} rows attempted, {total - n_failed} enriched, {n_failed} to DLQ "
          f"(model={model_id}, prompt={PROMPT_VERSION})", flush=True)
    if total >= 20 and n_failed / total > MAX_FAILURE_RATE:
        # Systemic failure (model down, prompt broken): fail the task, alert, don't publish.
        raise SystemExit(f"circuit breaker: {n_failed}/{total} failed")


if __name__ == "__main__":
    main()
