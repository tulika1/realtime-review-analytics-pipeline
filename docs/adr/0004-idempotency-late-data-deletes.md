# ADR-0004: Idempotency, ordering, late data and deletes

**Status:** Accepted

## Context
Delivery is **at least once** (producer retries, replays). Events arrive **out of order**: mobile clients queue reviews offline, and the producer simulates events up to 48 hours late. Reviews get edited and deleted. Any Airflow task may be retried or cleared and re-run.

## Decision
1. **Bronze is append-only**, partitioned by *ingest* date. Late events land in today's partition, so old partitions are never rewritten.
2. **Silver reads bronze incrementally** (Delta streaming source + checkpoint), so each run only processes new rows. A crashed run resumes where it stopped.
3. **Dedup on `event_id`** absorbs redelivery.
4. **Latest wins per `review_id`** by `(event_time, event_id)`. `event_id` breaks ties so the result is deterministic.
5. **The MERGE only accepts strictly newer events.** Replaying a batch changes nothing (idempotent). An old late event can't overwrite newer state, so arrival order doesn't matter. Tests: `test_spark_merge_is_idempotent_and_order_independent` (Spark) and `test_merge_is_idempotent_and_order_independent` (Python reference).
6. **Deletes become tombstones** (`is_deleted = true`). They propagate: gold drops the review, and enrichment skips it. A physical purge job (roadmap) removes the tombstones and the raw bronze payloads after 30 days.
7. **Quarantine appends are idempotent** within `foreachBatch` via Delta's `txnAppId`/`txnVersion`, so a retried micro-batch isn't written twice.

## Why SCD1, not SCD2
Consumers need the current opinion. SCD2 would double storage and make every join harder for a question nobody has asked yet. Delta time travel covers short-term "as of" questions. **When to revisit:** an analyst needs a sentiment history beyond the time-travel retention. Then add an SCD2 table built from the same bronze events. That's additive, and existing consumers don't change.

## Two implementations, one behaviour
`reviewlens.transform.to_silver` (pure Python) is the readable reference implementation. `reviewlens.spark.bronze_to_silver` is the scalable one. `test_spark_silver_matches_python_reference` asserts they produce identical silver state.
