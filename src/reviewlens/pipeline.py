"""Lite version of the pipeline in plain Python + DuckDB (no Docker/Spark). Used in CI.

    python -m reviewlens.pipeline
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from reviewlens.enrich import PROMPT_VERSION, EnrichmentCache, RuleBasedEnricher, enrich_rows
from reviewlens.generator import generate
from reviewlens.quality import gate, run_checks
from reviewlens.rag import HashingEmbedder, LocalVectorIndex
from reviewlens.transform import to_silver

ROOT = Path(__file__).resolve().parents[2]
DATA = Path(os.environ.get("REVIEWLENS_DATA", ROOT / "data"))
LAKE = DATA / "lake"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def build_gold(con: duckdb.DuckDBPyConnection, silver_path: Path, enriched_path: Path) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE fct_review AS
        SELECT s.review_id, s.product_id, s.rating,
               CAST(s.event_time AS TIMESTAMPTZ) AS event_time,
               e.sentiment, e.sentiment_score, e.topics, e.is_actionable, e.summary,
               e.model_id, e.prompt_version
        FROM read_json_auto('{silver_path.as_posix()}') s
        JOIN read_json_auto('{enriched_path.as_posix()}') e
          ON e.review_id = s.review_id AND e.text_hash = s.text_hash
        WHERE NOT s.is_deleted
    """)
    con.execute("""
        CREATE OR REPLACE TABLE mart_product_health AS
        SELECT product_id,
               COUNT(*)                                   AS reviews,
               ROUND(AVG(rating), 2)                      AS avg_rating,
               ROUND(AVG(sentiment_score), 2)             AS avg_sentiment,
               ROUND(AVG(is_actionable::INT) * 100, 1)    AS pct_actionable
        FROM fct_review GROUP BY product_id ORDER BY pct_actionable DESC
    """)
    con.execute("""
        CREATE OR REPLACE TABLE mart_topic_issues AS
        SELECT product_id, topic, COUNT(*) AS actionable_mentions
        FROM fct_review, UNNEST(topics) AS t(topic)
        WHERE is_actionable
        GROUP BY ALL ORDER BY actionable_mentions DESC
    """)


def _print_table(con: duckdb.DuckDBPyConnection, title: str, sql: str) -> None:
    rel = con.sql(sql)
    print(f"\n{title}\n  " + " | ".join(rel.columns))
    for row in rel.fetchall():
        print("  " + " | ".join(str(v) for v in row))


def run(llm: str = "local", n_reviews: int = 200, question: str | None = None) -> dict:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    if LAKE.exists():
        shutil.rmtree(LAKE)

    # bronze
    events = generate(n_reviews)
    _write_jsonl(LAKE / "bronze" / "reviews" / f"ingest_date={run_id[:8]}" / f"{run_id}.jsonl", events)

    # silver
    silver = to_silver(events)
    silver_path = LAKE / "silver" / "reviews_current.jsonl"
    _write_jsonl(silver_path, silver.rows)
    _write_jsonl(LAKE / "quarantine" / f"{run_id}.jsonl", silver.quarantine)

    # enrichment
    if llm == "bedrock":
        from reviewlens.enrich import ClaudeBedrockEnricher
        enricher = ClaudeBedrockEnricher()
    else:
        enricher = RuleBasedEnricher()
    cache = EnrichmentCache(DATA / "cache" / "enrichment.json")
    outcomes = enrich_rows(silver.rows, enricher, cache)
    hashes = {r["review_id"]: r["text_hash"] for r in silver.rows}
    enriched = [
        {"review_id": o.review_id, "text_hash": hashes[o.review_id], "model_id": enricher.model_id,
         "prompt_version": PROMPT_VERSION, **o.insight.model_dump()}
        for o in outcomes if o.insight
    ]
    enriched_path = LAKE / "silver" / "reviews_enriched.jsonl"
    _write_jsonl(enriched_path, enriched)
    _write_jsonl(LAKE / "dlq" / f"{run_id}.jsonl",
                 [{"review_id": o.review_id, "error": o.error} for o in outcomes if o.error])

    # quality checks
    baseline = None
    if llm != "local":
        rules = RuleBasedEnricher()
        baseline = {r["review_id"]: rules.enrich(r["review_text_redacted"], r["rating"]).sentiment
                    for r in silver.rows if not r["is_deleted"]}
    checks = run_checks(len(events), len(silver.quarantine), silver.rows, outcomes, baseline)
    published = gate(checks)

    # gold, only if checks passed
    con = duckdb.connect(str(LAKE / "gold.duckdb"))
    if published:
        build_gold(con, silver_path, enriched_path)

    # search index
    embedder, index = HashingEmbedder(), LocalVectorIndex()
    by_id = {r["review_id"]: r for r in silver.rows}
    for row in silver.rows:
        if row["is_deleted"]:
            index.delete(row["review_id"])
            continue
        index.upsert(row["review_id"], embedder.embed(row["review_text_redacted"]),
                     {"product_id": row["product_id"], "text": row["review_text_redacted"]})

    report = {
        "run_id": run_id,
        "events_in": len(events),
        "quarantined": len(silver.quarantine),
        "duplicates_dropped": silver.duplicates_dropped,
        "silver_rows": len(silver.rows),
        "tombstones": sum(r["is_deleted"] for r in silver.rows),
        "enriched": len(enriched),
        "cache_hits": sum(o.cached for o in outcomes),
        "dlq": sum(1 for o in outcomes if o.error),
        "checks": [c.__dict__ for c in checks],
        "published_to_gold": published,
        "vectors": len(index),
    }

    print(json.dumps({k: v for k, v in report.items() if k != "checks"}, indent=2))
    for c in checks:
        print(f"  [{'PASS' if c.passed else 'FAIL'}] {c.name:<28} {c.value:.3f}  ({c.threshold}, {c.severity})")
    if published:
        _print_table(con, "mart_product_health", "SELECT * FROM mart_product_health")
        _print_table(con, "mart_topic_issues (top 5)", "SELECT * FROM mart_topic_issues LIMIT 5")

    q = question or "Why are customers unhappy with charging?"
    hits = index.query(embedder.embed(q), k=3)
    print(f"Q: {q}")
    for h in hits:
        print(f"  {h.review_id} ({h.score:.2f}) {by_id[h.review_id]['review_text_redacted'][:90]}")
    if llm == "bedrock":
        from reviewlens.rag import answer_with_claude
        print("A:", answer_with_claude(q, hits))
    con.close()
    return report


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--llm", choices=["local", "bedrock"], default="local")
    p.add_argument("--reviews", type=int, default=200)
    p.add_argument("--question")
    a = p.parse_args()
    run(a.llm, a.reviews, a.question)


if __name__ == "__main__":
    main()
