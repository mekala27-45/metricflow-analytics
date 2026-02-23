-- RFM (Recency, Frequency, Monetary) segmentation
with user_metrics as (
    select user_id, days_inactive as recency_days, total_active_days as frequency,
        lifetime_revenue as monetary, engagement_score
    from {{ ref('dim_users') }}
    where total_payments > 0
),
scored as (
    select *,
        ntile(5) over (order by recency_days desc) as r_score,
        ntile(5) over (order by frequency) as f_score,
        ntile(5) over (order by monetary) as m_score
    from user_metrics
)
select *,
    r_score * 100 + f_score * 10 + m_score as rfm_score,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
        when r_score >= 3 and f_score >= 3 then 'Loyal Customers'
        when r_score >= 4 and f_score <= 2 then 'New Customers'
        when r_score <= 2 and f_score >= 3 then 'At Risk'
        when r_score <= 2 and f_score <= 2 then 'Lost'
        when f_score >= 4 then 'Potential Loyalists'
        else 'Need Attention'
    end as rfm_segment
from scored
