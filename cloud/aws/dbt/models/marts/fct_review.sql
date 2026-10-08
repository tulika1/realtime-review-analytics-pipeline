-- One row per live (non-deleted) review with its AI enrichment.
-- Tombstoned reviews disappear here, which is how a GDPR delete reaches every mart.
select
    s.review_id,
    s.product_id,
    s.rating,
    s.event_ts,
    date(s.event_ts)         as review_date,
    e.sentiment,
    e.sentiment_score,
    e.topics,
    e.is_actionable,
    e.summary,
    e.model_id,
    e.prompt_version
from {{ source('silver', 'reviews_current') }} s
join {{ source('silver', 'reviews_enriched') }} e
  on e.review_id = s.review_id
 and e.text_hash = s.text_hash      -- never join a stale enrichment to edited text
where not s.is_deleted
