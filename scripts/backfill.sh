#!/usr/bin/env bash
# Replay bronze for a date range: one Step Functions execution per ingest_date,
# at most $MAX_PARALLEL running at once. Safe to re-run: every stage is idempotent
# and the enrichment cache means already-enriched text is never paid for twice.
# Usage: scripts/backfill.sh 20260901 20260930
set -euo pipefail
START=$1; END=$2; MAX_PARALLEL=${MAX_PARALLEL:-3}
SFN_ARN=${SFN_ARN:?set SFN_ARN to the pipeline state machine ARN}

d=$START
while [[ "$d" -le "$END" ]]; do
  while [[ $(aws stepfunctions list-executions --state-machine-arn "$SFN_ARN" \
             --status-filter RUNNING --query 'length(executions)') -ge $MAX_PARALLEL ]]; do
    sleep 30
  done
  aws stepfunctions start-execution --state-machine-arn "$SFN_ARN" \
    --name "backfill-$d-$(date +%s)" --input "{\"ingest_date\":\"$d\",\"mode\":\"backfill\"}"
  d=$(date -d "$d + 1 day" +%Y%m%d)
done
