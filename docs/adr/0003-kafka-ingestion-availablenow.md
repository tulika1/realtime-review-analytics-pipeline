# ADR-0003: Kafka → bronze with Structured Streaming `availableNow`, triggered by Airflow

**Status:** Accepted

## Context
Events arrive continuously in Kafka. Consumers need data in gold within about 30 minutes, not seconds. Ingestion must not lose or double-count events across crashes and retries.

## Options
| Option | Freshness | Delivery guarantee | Cost on a laptop |
|---|---|---|---|
| Always-on Spark streaming job (`processingTime` trigger) | Seconds | Exactly-once into Delta | One JVM running all the time (~1 GB) |
| Plain Python consumer committing offsets → files | Seconds | At-least-once unless offsets and files commit together, which is hard | Low |
| **Spark streaming with `trigger(availableNow=True)`, run by Airflow** | Schedule interval (15 min) | **Exactly-once into Delta**: offsets and data commit together in the checkpoint + Delta log | JVM only while the task runs |
| Kafka Connect S3 sink | Minutes | At-least-once / exactly-once (depends on the connector) | Another JVM service |

## Decision
`availableNow`. Each run reads every offset since the last checkpoint, writes it to bronze, and stops. It's streaming semantics on a batch schedule.

## Details
- **Kafka key = `review_id`.** All events for a review land in the same partition, in order. The pipeline still doesn't *rely* on that order (see ADR-0004), because retries and late mobile clients break it anyway.
- **Bronze stores the raw payload string plus partition and offset.** Bronze never rejects data. Parsing and validation happen in silver, so a parsing bug is fixed by replaying bronze.
- **Producer:** `acks=all` and idempotence are enabled. The pipeline still assumes at-least-once delivery and dedups by `event_id`.
- `maxOffsetsPerTrigger` bounds memory when catching up on a large backlog.
- Kafka keeps data for 7 days, which is the replay window if bronze is ever lost.

## When to revisit
A freshness requirement under about 5 minutes (for example real-time moderation). Then run the same job with a `processingTime` trigger as an always-on service, and keep Airflow for the downstream batch stages.
