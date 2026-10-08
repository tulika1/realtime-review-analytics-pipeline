# AWS reference design (optional, costs money)

Not needed to run ReviewLens. The free local stack is in the repo root.

This folder shows how the same pipeline would run in production on AWS:
Firehose → S3 bronze → Glue 5 (Spark + Iceberg MERGE) → Lambda + Claude on Bedrock (enrichment) → dbt on Athena → S3 Vectors, orchestrated by Step Functions and deployed with Terraform.

- [architecture.md](docs/architecture.md): requirements, data flow, failure modes, scaling
- [docs/adr](docs/adr): the AWS-specific trade-offs (Iceberg vs Redshift, Step Functions vs MWAA, ...)
- [cost-model.md](docs/cost-model.md): AI and platform costs per month, and how to choose a model
- `infra/terraform`, `glue`, `orchestration`, `dbt`, `lambda`: code for the core path. Not deployed or tested end to end.
