# ADR-0001: Delta Lake as the table format

**Status:** Accepted

## Context
Silver needs upserts (edits), deletes (GDPR) and atomic publishes. Spark writes the tables, and a lightweight dashboard with no Spark reads them.

## Options
| Option | Pros | Cons |
|---|---|---|
| **Delta Lake** | ACID MERGE; idempotent writes from streaming micro-batches (`txnAppId`/`txnVersion`); time travel; first-class in Spark; readable from Python without Spark (delta-rs) | Ecosystem is Spark/Databricks-centred |
| Apache Iceberg | Engine-neutral; the native AWS choice (Athena, Glue) | Needs a catalog service even locally; heavier setup on a laptop |
| Plain Parquet | Simplest | No transactions. Edits and deletes mean rewriting partitions, and readers can see half-written data |

## Decision
Delta Lake, using path-based tables with no catalog service, so there's nothing extra to run.

## Consequences
- Overwriting gold is atomic, which is what makes write-audit-publish safe for dashboard readers.
- Time travel answers "what did gold look like before last night's run?"
- **On AWS** I'd pick Iceberg + Glue Catalog for Athena support ([cloud/aws ADR](../../cloud/aws/docs/adr/0001-lakehouse-iceberg.md)). The pipeline semantics (MERGE, tombstones, WAP) carry over unchanged.
