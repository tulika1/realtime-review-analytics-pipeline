# ADR-0004: Vector store for semantic search over reviews

**Status:** Accepted

## Context
About 5M vectors (256 dimensions), growing by about 10k a day. Queries come from internal users: roughly 500 a day, with p95 latency under 1 second being acceptable. We need metadata filters (`product_id`, date) and **real deletes** for GDPR.

## Options
| Option | Monthly cost at our scale | Strengths | Weaknesses |
|---|---|---|---|
| **Amazon S3 Vectors** | Very low: storage plus per-query fees, nothing charged while idle | Serverless, filters on metadata, lives next to the lake | Higher query latency than in-memory engines; no hybrid keyword + vector (BM25) search |
| OpenSearch Serverless | Hundreds of dollars a month minimum for compute units | Hybrid search, aggregations, millisecond latency | Expensive when idle; one more cluster-like thing to run |
| pgvector on Aurora | Instance cost | SQL joins with metadata; transactional deletes | Index tuning and memory sizing at 5M+ vectors; vertical scaling |
| Bedrock Knowledge Bases | Pass-through to the chosen store | Managed chunking and sync | Less control over IDs, deletes and re-embedding strategy |

## Decision
S3 Vectors. At internal-tool query volumes, cost is driven by idle capacity, not query speed. The vector key is the `review_id`, so upserts and tombstone deletes are simple key operations driven by silver.

## Consequences
- The embedding model and dimension are recorded with the index. Changing them means re-embedding everything into a **new** index and switching readers over to it (blue/green).
- **When to revisit:** customer-facing search (latency), or a requirement for keyword + semantic hybrid ranking. Then move to OpenSearch Serverless. The sync Lambda is the only writer, so only it changes.
