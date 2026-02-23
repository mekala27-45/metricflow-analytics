-- MRR Waterfall: New + Expansion - Contraction - Churn
with periods as (select * from {{ ref('int_subscription_periods') }})
select
    date_trunc('month', started_at) as month,
    sum(case when mrr_change_type = 'new' then monthly_price else 0 end) as new_mrr,
    sum(case when mrr_change_type = 'expansion' then monthly_price - coalesce(prev_price, 0) else 0 end) as expansion_mrr,
    sum(case when mrr_change_type = 'contraction' then coalesce(prev_price, 0) - monthly_price else 0 end) as contraction_mrr,
    sum(case when mrr_change_type = 'churn' then coalesce(prev_price, monthly_price) else 0 end) as churned_mrr,
    count(distinct case when mrr_change_type = 'new' then user_id end) as new_customers,
    count(distinct case when mrr_change_type = 'churn' then user_id end) as churned_customers
from periods group by 1
