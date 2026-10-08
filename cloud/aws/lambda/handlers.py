"""Lambda entry point for the Step Functions Distributed Map enrichment step.

Input (from ItemBatcher): {"Items": [silver rows...]}
Output: enriched rows written to s3://<lake>/silver/reviews_enriched_staging/, DLQ rows to dlq/.
The DynamoDB cache makes a retried batch nearly free.
"""
from __future__ import annotations

import json
import os
import time
import uuid

import boto3

from reviewlens.enrich import PROMPT_VERSION, ClaudeBedrockEnricher, ReviewInsight, enrich_one

_s3 = boto3.client("s3")
_cache = boto3.resource("dynamodb").Table(os.environ.get("CACHE_TABLE", "reviewlens-enrichment-cache"))
_enricher = None  # created once per warm container


def handler(event, _context):
    global _enricher
    _enricher = _enricher or ClaudeBedrockEnricher()
    bucket = os.environ["LAKE_BUCKET"]
    ok, failed = [], []

    for row in event["Items"]:
        key = f"{PROMPT_VERSION}:{_enricher.model_id}:{row['text_hash']}"
        cached = _cache.get_item(Key={"cache_key": key}).get("Item")
        if cached:
            insight = ReviewInsight.model_validate_json(cached["insight"])
        else:
            outcome = enrich_one(row, _enricher, max_attempts=3)
            if outcome.insight is None:
                failed.append({"review_id": row["review_id"], "error": outcome.error})
                continue
            insight = outcome.insight
            _cache.put_item(Item={"cache_key": key, "insight": insight.model_dump_json(),
                                  "expires_at": int(time.time()) + 180 * 86400})
        ok.append({"review_id": row["review_id"], "text_hash": row["text_hash"],
                   "model_id": _enricher.model_id, "prompt_version": PROMPT_VERSION,
                   **insight.model_dump()})

    batch_id = uuid.uuid4().hex
    for prefix, rows in (("silver/reviews_enriched_staging", ok), ("dlq/enrichment", failed)):
        if rows:
            _s3.put_object(Bucket=bucket, Key=f"{prefix}/{batch_id}.jsonl",
                           Body="\n".join(json.dumps(r) for r in rows).encode())
    if len(failed) > len(event["Items"]) // 2:
        raise RuntimeError(f"{len(failed)} of {len(event['Items'])} rows failed")  # counts toward Map tolerance
    return {"enriched": len(ok), "failed": len(failed)}
