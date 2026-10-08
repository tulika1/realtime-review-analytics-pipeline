# ADR-0002: Step Functions instead of MWAA (managed Airflow)

**Status:** Accepted

## Context
The pipeline has about six steps. Each step is an AWS service call: Glue, Lambda, Athena, ECS for dbt, and SNS. One step needs to fan out to thousands of parallel model calls with a concurrency limit and a failure-tolerance percentage.

## Options
| | Step Functions | MWAA (Airflow) | Dagster / Prefect Cloud |
|---|---|---|---|
| Idle cost | ~$0 (pay per state transition) | ~$350+/month minimum environment | SaaS fee + agents |
| Fan-out to 10k items | Distributed Map with native batching, concurrency limit and tolerated-failure % | Dynamic task mapping works, but the scheduler becomes the bottleneck | Good |
| Native AWS integrations | `.sync` for Glue, ECS, Athena; IAM per state machine | Through operators; one shared execution role | Through libraries |
| Developer experience | JSON/ASL is verbose; weaker local testing | Python DAGs; large ecosystem; familiar to most teams | Strongest asset/lineage model |
| Backfills | Start an execution per date range | `airflow dags backfill` is first-class | Partitioned assets are first-class |

## Decision
Step Functions. The workload is AWS-native and runs on a schedule. The Distributed Map handles the AI fan-out, with throttling and failure tolerance, without code we'd have to maintain. Paying ~$4k/year for an Airflow environment to run six tasks isn't justified.

## Consequences
- Backfills are scripted: `scripts/backfill.sh 20260901 20260930` starts one execution per day, at most N running at once.
- **When to revisit:** more than about 15 pipelines with cross-pipeline dependencies, or a team that already runs Airflow. Then MWAA (or Dagster for asset lineage) becomes worth its fixed cost. The stage code doesn't change, only the orchestration wrapper around it.
