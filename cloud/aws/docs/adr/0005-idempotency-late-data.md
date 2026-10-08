# ADR-0005: Idempotency, ordering, late data and deletes

**Status:** Accepted

## Context
The source delivers **at least once**, so duplicates happen. Events arrive **out of order**: mobile clients queue them offline, and they can be up to 72 hours late (see the contract's `x-slo`). Reviews are edited and deleted. Any stage can be retried, and any day may need to be replayed.

## Decision
1. **Bronze is immutable and partitioned by ingest date.** Late events land in today's partition, so old partitions are never rewritten. Replaying a day is always safe.
2. **Event-level dedup** on `event_id` absorbs redelivery.
3. **Latest wins** per `review_id`, ordered by `(event_time, event_id)`. `event_id` breaks ties so the result is deterministic.
4. **The MERGE only accepts strictly newer events.** That makes the merge idempotent (re-running a batch has no effect) and **independent of arrival order** (old late events can't overwrite newer state). Both properties are tested in `tests/test_transform.py`.
5. **Deletes become tombstones** (`is_deleted = true`) instead of physical deletes in silver. That way the delete *propagates*: gold filters it out, the vector index deletes the key, and the enrichment stage skips it. A scheduled purge physically removes tombstoned rows and the matching raw bronze objects within 30 days.

## Why SCD1 and not SCD2
Consumers need the *current* opinion. SCD2 would double storage and make every join more complex for a question nobody has asked yet. Iceberg time travel covers the short-term "what did it look like last week?" question. **When to revisit:** an analyst needs "sentiment trend as of date X" for more than 7 days back. Then add an SCD2 history table built from the same events. It's additive, and current consumers don't change.

## Watermark
Events older than 72 hours past the contract limit are still merged correctly (latest-wins doesn't need a watermark). But daily gold aggregates for closed days are only recomputed for a 3-day lookback window. Older corrections show up in the next monthly full refresh. This is a deliberate trade-off between correctness and compute cost, and it's documented for consumers.
