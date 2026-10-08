# ADR-0006: Micro-batch every 15 minutes instead of true streaming

**Status:** Accepted

## Context
Freshness SLO: gold at most 30 minutes behind the source at the 95th percentile. Nobody needs results within seconds. The ops "defect spike" alert is fine with a ~30 minute delay.

## Options
| Option | Freshness | Operating cost | Complexity |
|---|---|---|---|
| **Firehose → S3 + Step Functions every 15 minutes** | ~5 min buffer + ≤15 min wait + ~8 min run ≈ 28 min worst case | Low: pay per run | Low: batch semantics, easy replay |
| Kinesis Data Streams + Managed Flink | Seconds | Kinesis shards plus Flink compute running all the time | State management, checkpoints, exactly-once sinks into Iceberg |
| Spark Structured Streaming on EMR/Glue streaming | ~1 min | Cluster always on | Medium |

## Decision
Micro-batch. It meets the SLO at a fraction of the cost and keeps a single code path for incremental runs and backfills. The model-call stage is also batch-friendly: batching improves throughput under rate limits.

## Consequences
- The 30-minute SLO has very little headroom. An alert fires when p95 freshness goes above 25 minutes, using `dbt source freshness` plus a CloudWatch metric based on `updated_at`.
- **When to revisit:** a requirement for freshness under 5 minutes, such as real-time moderation of reviews before they're published. That's a different product. It would run a streaming path through Kinesis + Flink + a synchronous model call for the moderation decision only, and keep this batch lakehouse for analytics.
