-- Subscription periods with MRR change tracking
with subs as (select * from {{ ref('stg_subscriptions') }}),
plan_details as (select * from {{ ref('plan_details') }})
select s.*, p.tier_order,
    lag(s.plan_name) over (partition by s.user_id order by s.started_at) as prev_plan,
    lag(s.monthly_price) over (partition by s.user_id order by s.started_at) as prev_price,
    case
        when lag(s.subscription_id) over (partition by s.user_id order by s.started_at) is null then 'new'
        when s.monthly_price > coalesce(lag(s.monthly_price) over (partition by s.user_id order by s.started_at), 0) then 'expansion'
        when s.monthly_price < lag(s.monthly_price) over (partition by s.user_id order by s.started_at) then 'contraction'
        when s.change_type = 'churned' then 'churn'
        else 'renewal'
    end as mrr_change_type
from subs s left join plan_details p on s.plan_name = p.plan_name
