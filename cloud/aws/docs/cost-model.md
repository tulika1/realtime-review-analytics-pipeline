# Cost model

Being able to estimate the bill before building is a senior skill. These numbers are back-of-envelope and use **Anthropic first-party list prices per million tokens**. Claude on Amazon Bedrock is priced separately by AWS, so check the [Bedrock pricing page](https://aws.amazon.com/bedrock/pricing/) before quoting real numbers.

## Assumptions
- 10k new or edited reviews a day. 40% of them hit the enrichment cache (duplicates, rating-only edits), so about **6k model calls a day**.
- About 350 input tokens per call (system prompt + review) and about 250 output tokens (JSON plus some reasoning at effort `low`).
- Backfill: 5M historical reviews, about 30% deduplicated by the cache, so **3.5M calls**.

## AI enrichment cost per model
| Model ($ in / out per 1M tokens) | Per review | Incremental / month | 5M backfill (on-demand) | Backfill via batch inference (~50%) |
|---|---|---|---|---|
| Claude Opus 5.5 ($4 / $20) | ~$0.0064 | ~$1,150 | ~$22,400 | ~$11,200 |
| Claude Sonnet 5.5 ($2 / $10) | ~$0.0032 | ~$580 | ~$11,200 | ~$5,600 |
| Claude Haiku 5.5 ($0.10 / $0.50) | ~$0.00016 | ~$29 | ~$560 | ~$280 |

**How we choose:** the model is a configuration value. Run the golden set (200 hand-labelled reviews) on each model and pick the cheapest one within 2 percentage points of the best accuracy. A ~40× cost difference has to be justified by measured quality, not assumed. Two other levers:
1. **The cache** saves more than any prompt tuning. It's the first thing to check when the bill moves.
2. **Prompt caching doesn't help here.** The system prompt is shorter than the minimum length that can be cached. Saying so is part of the analysis.

## Platform cost (dev account, running every 15 minutes)
| Component | Driver | Estimate / month |
|---|---|---|
| Glue (2 DPU, ~3 min, 96 runs/day) | $0.44 per DPU-hour | ~$125. The biggest non-AI cost. **For small volumes, run the silver MERGE in Athena instead** (Athena supports Iceberg MERGE, pay per scan) and use Glue only for backfills |
| Step Functions Standard + Distributed Map | per state transition | < $5 |
| Lambda, DynamoDB on-demand, Firehose, SNS | requests and GB | < $10 |
| Athena | $5 per TB scanned, partition-pruned | < $5 |
| S3 + S3 Vectors | GB stored + requests | < $5 |

**For a portfolio:** set the schedule to `rate(1 day)`, run 1k reviews through the AI model once (cents to a few dollars depending on the model), take screenshots, then `terraform destroy`. The AWS Budget in `main.tf` emails you at 80% of forecast spend.
