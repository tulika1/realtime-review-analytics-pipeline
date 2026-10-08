# Presenting ReviewLens in an interview

## The 2-minute pitch (practise it out loud until it takes 2 minutes)

> "ReviewLens is an end-to-end pipeline that turns a messy stream of customer review events into product-health metrics: which products are getting worse, and why.
>
> Events go into Kafka, keyed by review ID. An Airflow DAG runs four Spark stages every 15 minutes. First, Kafka to a raw bronze Delta table using Structured Streaming with an `availableNow` trigger. That gives exactly-once ingestion through the checkpoint without paying for an always-on consumer. Then validation against a data contract, with bad records quarantined, and a MERGE into silver that only accepts newer events, so replays and out-of-order data give the same result. I test that in Spark against a pure-Python reference. Then enrichment: sentiment and defect topics, done only for new or changed text, with a DLQ and a circuit breaker. Finally gold, using write-audit-publish: quality checks run on an audit copy, and the gold tables the dashboard reads are only overwritten if the checks pass.
>
> It all runs free on my 8 GB laptop. That was a deliberate sizing decision: Spark runs in local mode inside the Airflow task rather than as a cluster, and I wrote that and the other trade-offs down as decision records, with a mapping to MSK, EMR and MWAA for production."

Then **stop and let them choose what to dig into.**

## Questions you'll get, and strong answers

**"Why Kafka here, and why key by `review_id`?"**
It decouples the producer from processing and gives 7 days of replay. Keying by `review_id` keeps all events for one review in the same partition and in order. But I don't *rely* on that order: retries and offline mobile clients break it anyway, so the MERGE compares `event_time` itself.

**"Exactly-once?"**
I'm precise about it. Kafka → bronze is exactly-once, because Spark commits offsets and the Delta write together in the checkpoint. Producer → Kafka is at-least-once in general, so silver deduplicates on `event_id`. And the MERGE only accepts strictly newer `(event_time, event_id)`. So the *effect* is exactly-once end to end, even though delivery isn't.

**"Why `availableNow` instead of a running streaming job?"**
The SLO is 30 minutes, not seconds. `availableNow` gives the same checkpoint guarantees, but the JVM only runs during the task. On my laptop that's the difference between fitting and not fitting. If they needed seconds, I'd run the same code with a `processingTime` trigger as a service. It's a one-line change.

**"Why not a Spark cluster?"**
At this volume a cluster adds about 2 GB of RAM and network overhead for no speedup. Local mode with 4 shuffle partitions is the right size. The master is a configuration value, so moving to EMR or Kubernetes doesn't change the jobs.

**"How do you handle late data?"**
Bronze is partitioned by ingest date, so late events never touch old partitions. Silver is latest-wins on event time, which needs no watermark, so it's always correct. In aggregates, a late event simply updates its day on the next gold rebuild.

**"How do you stop bad data reaching the dashboard?"**
At two gates. At silver, contract violations go to quarantine with the reasons, and are never silently dropped. At gold, write-audit-publish: blocking checks for uniqueness, enrichment coverage, quarantine rate and accepted values. If any fails, the task fails and the dashboard keeps the last good version. Every check result is stored in `_quality_runs`, which the dashboard shows.

**"What happens if enrichment breaks?"**
It's a separate task, so ingestion and silver keep going. Each row gets retries and then goes to the DLQ. If more than 5% fail, the circuit breaker fails the task, because that means the model or prompt is broken, not the data. Successful rows are saved first, so a re-run only redoes the failures. Only new or changed text is ever enriched, keyed on the text hash plus the prompt version.

**"How would you backfill or recover?"**
Bronze is the source of truth. Delete silver and its checkpoint, and the next run rebuilds it from bronze with the same result. That's what idempotency buys. Kafka's retention is a second replay window.

**"Take it to production on AWS."**
MSK for Kafka (replication factor 3, IAM auth), EMR Serverless or Glue for Spark, Delta or Iceberg on S3, MWAA for the same DAG (or Step Functions), and Secrets Manager. Add CloudWatch alarms on consumer lag and freshness. I have a costed reference design in `cloud/aws`.

**"What would you do differently?"**
"I'd add schema-registry-backed contracts (Avro or Protobuf) instead of JSON plus my own validation, and an SCD2 history table once someone needs sentiment trends beyond Delta's time-travel window."

**"What was hard?"**
Pick something real from building or running it. For example: making `foreachBatch` writes idempotent (`txnAppId`/`txnVersion`), or fitting Kafka, Spark and Airflow into 5 GB.

## Stories about ownership (STAR format; adapt them to your real job)
Prepare 4 stories from your own experience. Interviewers for senior roles probe for:
1. **An incident you owned end to end:** detection → mitigation → root cause → prevention you shipped.
2. **Pushing back on a requirement with data,** for example "real-time" when 30 minutes was enough.
3. **A standard others adopted,** such as data contracts, a testing convention or cost tagging.
4. **A trade-off you later reversed,** and the signal that told you to.

Structure: one sentence of context → your decision and the alternatives → a number for the impact → what you'd do differently.
