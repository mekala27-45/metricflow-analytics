-- North Star Metric framework
with daily_metrics as (
    select
        event_date as metric_date,
        count(distinct user_id) as dau,
        count(distinct case when is_high_value_action then user_id end) as core_action_users,
        count(*) as total_events
    from {{ ref('int_events_enriched') }}
    group by 1
),
revenue_daily as (
    select payment_date as metric_date, sum(recognized_mrr) as daily_mrr, count(distinct user_id) as paying_users
    from {{ ref('int_revenue_normalized') }} where payment_status = 'succeeded' group by 1
),
signups_daily as (
    select signup_date as metric_date, count(*) as new_signups
    from {{ ref('stg_users') }} group by 1
)
select
    d.metric_date,
    d.dau, d.core_action_users, d.total_events,
    coalesce(r.daily_mrr, 0) as daily_mrr,
    coalesce(r.paying_users, 0) as paying_users,
    coalesce(s.new_signups, 0) as new_signups,
    -- North Star: Weekly Active Core Users (rolling 7d)
    avg(d.core_action_users) over (order by d.metric_date rows between 6 preceding and current row) as north_star_wau_core,
    -- Growth rate
    d.dau::decimal / nullif(lag(d.dau, 7) over (order by d.metric_date), 0) - 1 as wow_dau_growth,
    -- Revenue growth
    coalesce(r.daily_mrr, 0) / nullif(lag(coalesce(r.daily_mrr, 0), 30) over (order by d.metric_date), 0) - 1 as mom_mrr_growth
from daily_metrics d
left join revenue_daily r on d.metric_date = r.metric_date
left join signups_daily s on d.metric_date = s.metric_date
