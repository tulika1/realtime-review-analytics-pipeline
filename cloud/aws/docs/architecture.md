# ReviewLens architecture

## 1. Problem and requirements

**Users.** Product managers want to see which products are getting worse and why. Operations wants defects they can act on within the hour. Analysts want SQL. Support leads want to ask questions in plain English.

**Functional requirements**
- Ingest review create, update and delete events from the Reviews service.
- Keep the current state of every review, and honour deletes (GDPR) in every downstream store, including the vector index.
- For each review, produce the sentiment, the defect topics, whether it's actionable, and a summary.
- Serve daily KPIs in SQL/BI, and answer plain-English questions with citations.

**Non-functional requirements (set these numbers before drawing boxes)**

| Requirement | Target | Drives |
|---|---|---|
| Volume | 10k new or edited reviews/day; 5M historical; 10× peak during sales | Micro-batch is enough (ADR-0006) |
| Freshness | Gold ≤ 30 minutes behind source at the 95th percentile | 15-minute micro-batch after Firehose buffering (ADR-0006) |
| Correctness | No duplicates in silver/gold; arrival order doesn't change results; deletes propagate within 24 hours | Idempotent MERGE, tombstones (ADR-0005) |
| AI cost | ≤ $X/month (see the [cost model](cost-model.md)); no tokens spent on unchanged text | Content-hash cache, deletes never enriched |
| Privacy | No raw PII leaves the account boundary or reaches the AI model | Redaction in silver before enrichment |
| Recoverability | Any day can be rebuilt from bronze | Immutable bronze, replayable orchestration |

## 2. Data flow

1. **Ingest.** The producer writes `ReviewEvent v1` to Firehose, which buffers for 5 minutes or 64 MB and writes gzipped JSON to `bronze/reviews/ingest_date=YYYYMMDD/`. Bronze is append-only. Partitioning by *ingest* date (not event date) means late events never rewrite old partitions.
2. **Silver (Glue/Spark, Iceberg).** The job validates each event against the contract and sends failures to quarantine with the reason. It then drops duplicate `event_id`s, keeps the latest event per `review_id` by (`event_time`, `event_id`), redacts PII and computes a `text_hash`. Finally it runs a `MERGE` into `silver.reviews_current` that only accepts strictly newer events. Deletes become tombstones rather than physical deletes, so downstream systems can see them.
3. **Select rows to enrich.** An Athena query selects live rows whose `(prompt_version, model, text_hash)` has no cached insight and writes them to an S3 manifest. This is where most of the cost saving happens: unchanged and duplicate texts cost nothing.
4. **Enrichment (Step Functions Distributed Map → Lambda → Claude on Bedrock).** Requests run in batches of 50 with at most 10 in parallel (plus a reserved-concurrency cap on the Lambda). Output must match a fixed JSON schema and is checked again with Pydantic. Throttling gets retries with jitter. Invalid output and model refusals go to the DLQ. If more than 5% of a batch fails, the run stops (circuit breaker), because that points to a broken model or prompt rather than bad data.
5. **Quality gate (dbt on Athena).** dbt builds into an `audit` schema and runs the tests: column contracts, uniqueness, accepted values, source freshness, and a check that the AI sentiment agrees with the star rating. Gold is published only if the blocking tests pass. Otherwise consumers keep yesterday's correct data and the on-call engineer is paged.
6. **Vectors.** Titan Text Embeddings v2 vectors (256 dimensions) are upserted for changed live reviews, and **deleted** for tombstones.
7. **Serving.** Athena/QuickSight read gold. The Q&A Lambda embeds the question, retrieves the top-k reviews filtered by product, and asks Claude to answer **only** from them, citing `[review_id]`.

## 3. Failure modes and what happens

| Failure | Detection | Behaviour |
|---|---|---|
| Producer sends a bad schema | Quarantine rate > 2% (blocking check) | Gold not published. The producing team is paged with sample rows |
| Same event delivered twice | `event_id` dedup + MERGE predicate | No effect |
| Events arrive out of order or days late | MERGE compares `event_time` | Older events can't overwrite newer state |
| Bedrock throttling | Retry with jitter in the state machine | Slower run, same result |
| Model returns malformed output | Schema check + Pydantic validation | Retry, then DLQ. Gold coverage check blocks if > 5% missing |
| Prompt regression after a `PROMPT_VERSION` bump | Shadow comparison with the rule-based baseline + rating-agreement check | Warning on the dashboard. Roll back the prompt version, and cached v(n-1) results are reused |
| Glue job fails halfway | Iceberg commits are atomic | Retry the whole job. Nothing partial is visible |
| Silver bug discovered later | n/a | Fix it, then replay bronze for the affected `ingest_date` range (idempotent) |
| Deletion request (GDPR) | Delete event becomes a tombstone | Removed from gold and the vector index on the next run. Bronze purged by a scheduled job within 30 days |

## 4. Scaling notes

- **10× volume.** Glue autoscaling covers the Spark side. On the AI side, the limits are Bedrock tokens-per-minute quotas and cost, not compute. Raise Map concurrency only after the quota increase is approved, and use **Bedrock batch inference** (asynchronous, about 50% cheaper) for backfills.
- **Backfill of 5M historical reviews.** Don't run it through the incremental path. Run a separate Step Functions execution reading a manifest in date slices, via Bedrock batch inference. It shares the same cache, so the incremental job skips anything the backfill already did.
- **Small files.** Firehose buffering plus scheduled Iceberg `rewrite_data_files` and `expire_snapshots` maintenance.
- **Hot keys.** Not a concern for `review_id` (high cardinality). Iceberg partitions by `bucket(16, review_id)` so MERGE pruning stays effective.

## Roadmap

1. Implement the `select-to-enrich`, `publish-gold`, `sync-vectors` and Q&A Lambdas.
2. Add an evaluation dataset: 200 hand-labelled reviews, with every prompt or model change scored against them in CI.
3. Change data feed from Iceberg to downstream consumers. SCD2 history table if analysts need "sentiment as of date".
4. OpenLineage events from Glue and dbt, so you can trace a gold column back to its source.
