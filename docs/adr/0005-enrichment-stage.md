# ADR-0005: Enrichment as a separate, cached, failure-isolated stage

**Status:** Accepted

## Context
Each review gets a sentiment, defect topics, an "is this actionable?" flag and a summary. The enricher is pluggable:
- `rules`: deterministic keywords. The default; instant and free.
- `ollama`: a free local LLM (for example Llama 3.2 3B) with schema-constrained JSON output.
- `bedrock`: Claude on AWS, the production option (paid; see `cloud/aws`).

An LLM enricher behaves like an unreliable upstream dependency. It's **slow, can fail or return invalid output, and its behaviour changes when the prompt or model changes.**

## Decision
1. **Its own Airflow task**, not inside the silver job. A model outage fails only `enrich_reviews`. Silver keeps ingesting, and gold keeps serving the last good version.
2. **Only new or changed text is enriched:** a left-anti join on `(review_id, text_hash)` for the current `prompt_version` + `model_id`. Retries, replays and rating-only edits cost nothing. Bumping `PROMPT_VERSION` deliberately re-enriches.
3. **Schema-constrained output plus Pydantic validation.** Topics are mapped to a fixed list and scores are clamped to the valid range.
4. **Per-row retries, then a DLQ table** with the error. One bad review never fails the batch.
5. **Circuit breaker:** if more than 5% of at least 20 rows fail, the task fails after saving the successful rows. That points to a systemic problem (model down, broken prompt), so it shouldn't be retried blindly.
6. **Lineage:** every insight row carries `model_id` and `prompt_version`. Gold joins on `text_hash`, so an insight is never paired with edited text.

## Consequences
- Enrichment runs inside `mapInPandas`, so the enricher is created once per partition, not once per row.
- The gold quality gate checks enrichment coverage (blocking, ≥ 95%) and agreement between sentiment and star rating (warning). A prompt or model regression shows up as a drop in agreement.
