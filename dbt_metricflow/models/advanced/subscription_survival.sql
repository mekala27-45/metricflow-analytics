-- Subscription survival curves
with subs as (
    select user_id, plan_name, duration_days, change_type,
        case when change_type = 'churned' then 1 else 0 end as is_censored_event
    from {{ ref('stg_subscriptions') }}
    where plan_name != 'free'
),
duration_buckets as (
    select plan_name,
        floor(duration_days / 30.0) as month_bucket,
        count(*) as total_at_risk,
        sum(is_censored_event) as churned_in_bucket
    from subs group by 1, 2
)
select *,
    1.0 - (churned_in_bucket::decimal / nullif(total_at_risk, 0)) as survival_rate_in_bucket,
    sum(churned_in_bucket) over (partition by plan_name order by month_bucket) as cumulative_churned
from duration_buckets
order by plan_name, month_bucket
