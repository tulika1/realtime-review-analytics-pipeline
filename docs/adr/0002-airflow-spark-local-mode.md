# ADR-0002: Airflow orchestration, Spark in local mode (no cluster)

**Status:** Accepted

## Context
Target machine: a laptop with 8 GB of RAM, of which Docker gets about 5 GB. Volume: a few events per second, so a few hundred thousand rows a day at most. The stack must include Kafka, Spark and Airflow, and still leave room for the OS.

## Options
| Option | RAM | Notes |
|---|---|---|
| Spark standalone cluster (master + 1–2 workers) + Airflow (separate scheduler, webserver, workers) | ~6–8 GB | Looks like production, but doesn't fit, and buys no performance at this volume |
| **Airflow `standalone` (one container) + Spark `local[2]` inside the task** | ~1.2 GB idle, ~2.5 GB while a Spark task runs | Fits. One JVM at a time |
| Airflow with KubernetesExecutor / Spark on Kubernetes | Even more | Right at scale, wrong here |

## Decision
- Airflow 3 `standalone` with LocalExecutor and Postgres. `parallelism=2` and `max_active_runs=1`.
- Each task starts Spark in local mode (`python -m reviewlens.spark.<stage>`), with a 1 GB driver and 4 shuffle partitions instead of the default 200.
- Container memory limits in `docker-compose.yml` make this sizing explicit.

## Consequences
- Each task pays about 10–20 seconds of JVM startup. That's acceptable on a 15-minute schedule.
- `max_active_runs=1` also stops two runs from merging into the same tables at once.
- **Scaling path:** set `SPARK_MASTER` to a real cluster (EMR, Dataproc, Kubernetes) or use `SparkSubmitOperator`. The stage code doesn't change. On AWS the orchestrator becomes MWAA or Step Functions ([trade-off](../../cloud/aws/docs/adr/0002-orchestration.md)).
