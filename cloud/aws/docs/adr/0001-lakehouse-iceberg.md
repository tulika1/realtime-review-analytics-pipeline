# ADR-0001: Iceberg lakehouse on S3 + Athena instead of Redshift

**Status:** Accepted · **Owner:** data-platform

## Context
We need tables that support MERGE (upserts and deletes), can be rebuilt from raw data, and can be read by several engines: Spark for processing, Athena for SQL, and possibly Snowflake or EMR later. Query load is light: around 20 analysts and one dashboard that refreshes hourly. Budget is tight.

## Options
| Option | Pros | Cons |
|---|---|---|
| **Iceberg on S3 + Glue Catalog + Athena** | Pay per query; open format any engine can read; ACID MERGE; time travel for debugging and audits; schema evolution | Table maintenance (compaction, expiring old snapshots) is ours to run; Athena slows down under high concurrent load |
| Redshift Serverless | Fast joins, good concurrency, mature BI integration | Minimum compute charge even when idle; data locked into one engine; MERGE on Redshift isn't cheaper |
| Delta Lake on EMR/Databricks | Mature MERGE, Change Data Feed | Extra platform and vendor; weaker native AWS integration than Iceberg |
| Plain Parquet + Hive partitions | Simplest | No ACID updates or deletes, so GDPR deletes and edits mean rewriting whole partitions |

## Decision
Iceberg v2 tables registered in the Glue Data Catalog. Writes go through Glue Spark, reads through Athena. Silver uses `merge-on-read` because MERGE runs often and rows are small. Gold marts use copy-on-write, since they're read-heavy and rebuilt by dbt.

## Consequences
- We own maintenance: a weekly `rewrite_data_files` (compaction) and `expire_snapshots` job, with 7-day retention for time travel.
- Time travel lets us answer "what did the dashboard show on Tuesday?" without keeping extra copies.
- **When to revisit:** more than about 50 concurrent BI users, or p95 dashboard query time above 5 seconds. Then put Redshift Serverless (Spectrum or zero-ETL) in front of gold only, and keep silver in Iceberg.
