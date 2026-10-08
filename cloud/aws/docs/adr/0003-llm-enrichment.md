# ADR-0003: How we put an LLM inside a data pipeline

**Status:** Accepted

## Context
Sentiment, defect topics and summaries need language understanding. The old keyword rules (still in `RuleBasedEnricher`) miss sarcasm, mixed reviews and new kinds of defects. An LLM is accurate but behaves like any external upstream dependency. It is **slow, rate-limited, paid per call, sometimes wrong, and its behaviour changes when the prompt or model changes.**

## Decisions and alternatives

### 1. Where enrichment runs
| Option | Verdict |
|---|---|
| Inside the Spark job (UDF calls Bedrock) | Rejected. Spark retries a whole task, so a failure re-bills every call already made in that partition. Rate limiting across executors is hard to control |
| Synchronously in the ingestion path (Lambda per event) | Rejected. It couples ingestion availability to the model's availability, and there's no batching |
| **Separate stage: Step Functions Distributed Map → Lambda** | **Chosen.** Independent retries, concurrency limit, failure-percentage threshold, and per-item DLQ |
| Bedrock batch inference (asynchronous, about 50% cheaper) | **Used for backfills.** Turnaround is up to 24 hours, so it doesn't meet the freshness SLO for incremental runs |

### 2. Controlling the output
- **Structured output** (`output_config.format` with a JSON schema) means the model's answer must match the schema. Pydantic still checks it again: topics are mapped to a fixed list, and scores are clamped to the allowed range. A wrong-but-valid answer is still possible. Quality checks catch that statistically.
- **Effort `low`.** This is a short classification task, so we don't pay for deep reasoning.
- **Refusals and truncation** are checked explicitly through `stop_reason`. A refusal is retried once on a fallback model (SDK refusal-fallback middleware), and then goes to the DLQ. It is never retried in a loop.

### 3. Idempotency and cost: the content-hash cache
Cache key = `prompt_version : model_id : sha256(redacted_text)`.
- Retries, replays and backfills never pay twice.
- Identical texts are enriched once. In the synthetic data, 165 of 198 rows hit the cache on the *first* run. Real data has a lot of "Great product!" too.
- An edit that changes the text gets a new hash, so it's re-enriched. An edit that only changes the rating doesn't.
- Bumping `PROMPT_VERSION` re-enriches deliberately. Rolling back reuses the old cached results for free.
- Gold joins enrichment on `review_id` **and** `text_hash`, so a stale insight can never be shown next to edited text.

### 4. Quality and drift
- **Blocking check:** enrichment coverage ≥ 95% of live rows.
- **Warning checks:** sentiment agrees with the star rating ≥ 80% of the time, and overlap with the rule-based baseline ≥ 60%. A sudden drop after a deploy points to a prompt or model regression.
- **Lineage:** every insight row carries `model_id` and `prompt_version`, so we can answer "which numbers came from the old prompt?"
- **Next step:** a 200-review golden set labelled by hand, with every prompt or model change scored against it in CI before it ships.

### 5. Privacy
Only PII-redacted text is sent, through Bedrock inside our AWS account. Customer IDs are never put in the prompt.

## Model choice
The default is `anthropic.claude-opus-5-5` at effort `low`. The model ID is a configuration value (`REVIEWLENS_MODEL`), so it's easy to compare models on the golden set. The [cost model](../cost-model.md) shows the price per review for each model. The decision rule: **pick the cheapest model whose golden-set accuracy is within 2 percentage points of the best one**, and record that measurement in this ADR.
