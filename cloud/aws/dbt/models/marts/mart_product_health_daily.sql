select
    product_id,
    review_date,
    count(*)                                              as reviews,
    round(avg(rating), 2)                                 as avg_rating,
    round(avg(sentiment_score), 2)                        as avg_sentiment,
    round(100.0 * avg(case when is_actionable then 1 else 0 end), 1) as pct_actionable
from {{ ref('fct_review') }}
group by 1, 2
