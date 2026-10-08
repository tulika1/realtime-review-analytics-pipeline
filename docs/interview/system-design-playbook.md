# Senior data engineer system design playbook

## The framework (45 minutes)

| Minutes | Step | What a senior candidate does |
|---|---|---|
| 0–7 | **Clarify** | Who consumes the data, and what decision does it drive? Volume, peak and growth. Freshness. Correctness (dupes OK? deletes?). Retention, PII, budget. **Write the numbers down.** |
| 7–10 | **Estimate** | Events/sec, GB/day, storage per year, number of model calls and cost. Rough math shows judgement. |
| 10–25 | **High-level design** | Sources → ingest → raw → clean → serve. Name the storage format, orchestrator and serving layer, each with one sentence of *why*. |
| 25–38 | **Deep dive** (let them pick, or offer your strongest area) | Idempotency, late data, schema evolution, backfills, data quality, partitioning, small files, AI stage reliability. |
| 38–45 | **Operate and evolve** | SLOs, alerts, on-call, cost guardrails, what breaks at 10×, and what you'd build next. |

**Phrases that signal seniority:** "What decision does this data drive?" · "I'd trade X for Y because the requirement says…" · "Here's when I'd revisit that" · "Who owns this contract?" · "How do we backfill this?" · "What does on-call see when this breaks?"

**Anti-patterns:** naming tools before requirements · "real-time" with no SLO number · claiming exactly-once without explaining how · no backfill story · no data-quality story · putting the AI model in the request path without a fallback.

---

## Practice problems (with the trade-offs interviewers want to hear)

### 1. Clickstream analytics for an e-commerce site (50k events/sec at peak)
- **Ingest:** Kinesis Data Streams (shards = peak MB/s ÷ 1) or MSK if Kafka skills or ecosystem matter. Firehose to S3 for the raw copy.
- **Trade-off:** Flink for sessionisation in seconds vs. a 5-minute micro-batch. Ask what the session data is *for*. Marketing attribution is fine daily; fraud isn't.
- **Deep dives:** late events and watermarks, bot filtering, PII in URLs, small files (compaction), partitioning by date and hour, never by user.
- **Serving:** Iceberg + Athena for ad-hoc queries; pre-aggregated rollups for dashboards (ClickHouse or Druid if sub-second at high concurrency is needed).

### 2. Change data capture from 200 OLTP tables into a lakehouse
- **Ingest:** DMS or Debezium on MSK Connect → S3 bronze; MERGE into Iceberg silver.
- **Trade-offs:** DMS (managed, weaker schema-change handling) vs. Debezium (flexible, more to operate). Merge-on-read vs. copy-on-write: write frequency vs. read latency.
- **Deep dives:** initial snapshot + CDC switch-over without gaps (mark the log position first, then snapshot); schema evolution (additive changes auto-applied, breaking changes go to quarantine and an alert); deletes; ordering by log position, never by timestamp.
- **Ownership:** per-table contracts, a freshness SLO per tier, and an onboarding template so new tables are config, not code.

### 3. AI document processing: 1M invoices/month (PDF) → structured accounts-payable data
- **Pipeline:** S3 upload → SQS → extraction workers (Claude with PDF input, schema-constrained output) → validation (totals add up, vendor exists, IDs match the master data) → human review queue for low-confidence results → warehouse.
- **Trade-offs:** model quality vs. cost per document (decide with a golden set); synchronous vs. batch inference (SLA hours, so batch is fine); OCR first vs. sending the PDF straight to the model.
- **Deep dives:** idempotency by document hash; confidence and business-rule checks decide automatic approval vs. human review; audit trail (model, prompt version, input hash, reviewer); PII and retention; cost guardrails.
- **What makes it senior:** "The model is never the system of record. Validated output plus an audit trail is."

### 4. RAG platform over company documents (Confluence, Drive, tickets)
- **Pipeline:** connectors → normalise → chunk (by structure, ~500–1000 tokens with overlap) → embed → vector store with access-control metadata → retrieve (+ re-rank) → generate with citations.
- **Trade-offs:** vector store (S3 Vectors vs. OpenSearch hybrid vs. pgvector); chunk size (recall vs. precision); re-embedding cost when the embedding model changes (blue/green indexes).
- **Deep dives:** **permissions** (filter on the user's groups at retrieval time, never trust the prompt to do it); incremental sync and deletes; freshness; evaluation (retrieval recall@k on labelled questions, answer faithfulness).
- **Trap:** forgetting deletes and permission changes. A deleted document that can still be retrieved is a data leak.

### 5. Feature store / ML training data with point-in-time correctness
- **Core idea:** training joins must use feature values *as of* the label timestamp, never later (no leakage).
- **Design:** offline store (Iceberg, SCD2 or event-sourced) + online store (DynamoDB/Redis) fed by the same transformation code; backfill via time travel.
- **Trade-offs:** build vs. SageMaker Feature Store or Feast; online consistency vs. cost.

### 6. Daily finance reporting that must reconcile to the cent
- **Priorities:** correctness and auditability over freshness.
- **Design:** immutable raw data, deterministic transforms, reconciliation checks against source totals as a **blocking** gate, write-audit-publish, versioned outputs, sign-off workflow.
- **Trade-off:** decimal types everywhere (no floats); re-runnable by `as_of_date`; SCD2 for dimensions.

---

## AWS deep-dive checklist (know one sentence + one gotcha for each)
- **S3:** request-rate limits per prefix; lifecycle tiers; versioning + Object Lock for audit.
- **Glue:** DPU pricing and per-minute billing; job bookmarks (and why MERGE idempotency is better than relying on them); Glue 5 + Iceberg.
- **Athena:** pay per TB scanned, so partition pruning, columnar formats and CTAS matter; Iceberg MERGE support; workgroup query limits as cost guardrails.
- **Redshift:** distribution and sort keys; Serverless RPU minimums; zero-ETL; Spectrum.
- **Kinesis:** shard limits (1 MB/s in, 2 MB/s out); enhanced fan-out; Firehose buffering vs. freshness.
- **Lake Formation:** column/row-level security, tag-based access control (LF-TBAC); cross-account sharing.
- **Step Functions vs. MWAA:** cost model, Distributed Map, `.sync` integrations.
- **Bedrock:** on-demand vs. provisioned throughput vs. batch inference; tokens-per-minute quotas; Guardrails; data stays in your account; model access per region.
- **IAM:** least privilege per job role; no wildcard `s3:*` on the lake; KMS key policies.
- **Cost:** tagging, Budgets, Cost Explorer by tag; the biggest bill is usually idle compute or full scans.

## 4-week prep plan
| Week | Focus | Output |
|---|---|---|
| 1 | Get ReviewLens running in Docker; screenshot the green Airflow DAG and the dashboard | GitHub repo + architecture image on your CV/LinkedIn |
| 2 | Run problems 1–3 above out loud, 45 minutes each, recorded | Notes on where you hesitated |
| 3 | SQL (window functions, dedup, gaps-and-islands, SCD2 MERGE) + Python (generators, dedup, retry logic) | 30 problems |
| 4 | Ownership stories (STAR) + mock interviews; problems 4–6 | 4 polished stories, 2 mocks |
