# SLOs, alerting and runbook

**Owner:** reviews-data-platform (you) · **Upstream:** the Reviews service team owns the `ReviewEvent v1` contract and the producer · **Downstream:** the product-health dashboard and analysts.

## Service level objectives
| SLI | SLO | How it's measured |
|---|---|---|
| Freshness: now − latest published gold run | ≤ 30 min, 95% of the time | `run_at` in `gold/_quality_runs` where `published = true` |
| Kafka consumer lag at the end of `kafka_to_bronze` | 0 after each run (it drains to the latest offset) | Streaming query progress in the task log |
| Completeness: live silver rows that are enriched | ≥ 95% on every publish | Blocking check `enrichment_coverage` |
| Correctness: duplicate `review_id` in gold | 0 | Blocking check `fct_review_id_unique` |
| Contract health: quarantined / bronze rows | ≤ 2% | Blocking check `contract_quarantine_rate` |
| Delete propagation | Tombstoned review absent from gold after the next run | Gold filter on `is_deleted`. Reconciliation query (roadmap) |

## Alerts
Locally, alerts are failed Airflow tasks (red in the UI). In production they'd go to Slack or PagerDuty via an `on_failure_callback`.

| Signal | Meaning | Runbook |
|---|---|---|
| `kafka_to_bronze` failed | Kafka unreachable or checkpoint problem | R1 |
| `bronze_to_silver` failed | Spark error in parsing or MERGE | R2 |
| `enrich_reviews` failed with "circuit breaker" | More than 5% of enrichments failed: model down or prompt broken | R3 |
| `gold_write_audit_publish` failed with "quality gate failed" | A blocking check failed, so gold was NOT published | R4 |

## Runbook
**R1: Kafka to bronze failed.** Run `docker compose ps kafka`. Is it healthy? Retries handle Kafka restarts. Offsets live in the checkpoint (`/opt/lake/_checkpoints/kafka_to_bronze`), so a re-run continues exactly where the last successful run stopped. **Never delete a checkpoint to "fix" a run.** That re-reads the topic from the earliest offset. It's safe (silver deduplicates), but slow.

**R2: Silver failed.** Read the Spark error in the task log. Because the stage is checkpointed and the MERGE is idempotent, fix the code (the `src/` folder is mounted, so no rebuild is needed) and **clear the task** in Airflow to re-run it.

**R3: Enrichment circuit breaker.** Query the `dlq/enrichment` table and group by `error`. If it's a connection error and `ENRICHER=ollama`, check that Ollama is running on the host. If it's a validation error, the prompt or model is producing bad output: roll back `PROMPT_VERSION`. The rows that succeeded are already saved, so a re-run only retries the rest.

**R4: Quality gate failed.** Gold still holds the last good data, so the dashboard is safe. Open `gold/_quality_runs` (shown at the bottom of the dashboard) or the task log to see which check failed.
- `contract_quarantine_rate`: query `quarantine/reviews`, group by `errors`, and send samples to the producing team. After their fix, the pipeline catches up on its own.
- `enrichment_coverage`: see R3.
- `fct_review_id_unique`: a logic bug. Stop and investigate before re-running.

## Replay and backfill
Bronze is the source of truth for everything downstream. To rebuild silver after a logic fix:
1. Pause the DAG.
2. Delete `silver/reviews_current` and `_checkpoints/bronze_to_silver`.
3. Unpause. The next run re-reads all of bronze and produces the same correct state, because the MERGE is idempotent and doesn't depend on arrival order.

## Change management
- Contract change: new major version (`review_event.v2`), a dual-publish window, and consumer sign-off.
- Prompt or model change: bump `PROMPT_VERSION` and record why in ADR-0005.
- Gold schema change: the Delta `overwriteSchema` is explicit in code. Announce it to dashboard owners before merging.
