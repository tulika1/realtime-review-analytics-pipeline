# ADR-0006: Local-first, cloud-ready

**Status:** Accepted

## Context
The project must cost $0 to run and still show how it would run in production on AWS.

## Decision
Everything runs locally in Docker. Each component maps to an AWS service, and the code is kept free of local-only assumptions: paths come from `LAKE_PATH`, the Spark master from `SPARK_MASTER`, and Kafka from `KAFKA_BOOTSTRAP`.

| Local | AWS equivalent | What changes |
|---|---|---|
| Kafka (KRaft, 1 broker) | Amazon MSK / MSK Serverless, or Kinesis | Bootstrap servers + IAM auth. Replication factor 3 |
| Spark local mode in Airflow | EMR Serverless or Glue 5 | `SPARK_MASTER` / job submission. Stage code unchanged |
| Delta tables in a Docker volume | Delta or Iceberg on S3 | `LAKE_PATH=s3a://...` + Hadoop S3A config. Iceberg if Athena is the reader |
| Airflow `standalone` + Postgres | MWAA, or Step Functions | DAG file unchanged on MWAA |
| Rules / Ollama enricher | Claude on Bedrock (`ENRICHER=bedrock`) | Credentials + cost controls ([cost model](../../cloud/aws/docs/cost-model.md)) |
| Streamlit on gold | QuickSight / Athena, or Streamlit on ECS | Read path |

A fuller AWS reference design (Terraform, Glue/Iceberg job, Step Functions state machine, Bedrock enrichment, cost model) is in [cloud/aws](../../cloud/aws). It's optional and costs money to deploy.

## Consequences
- What's shown locally is real (real Kafka, real Spark, real Delta, real Airflow). Only scale and high availability differ.
- In an interview: "Here's what changes for production: replication, IAM, secrets, monitoring, autoscaling, and what each costs."
