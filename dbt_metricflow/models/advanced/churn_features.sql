-- Churn prediction feature table
with lifecycle as (select * from {{ ref('int_user_lifecycle') }}),
health as (select user_id, health_score, sessions_last_30d from {{ ref('fct_health_scores') }}),
events_recent as (
    select user_id,
        count(*) as events_last_14d,
        count(distinct event_type) as unique_events_14d,
        count(case when event_type = 'support_ticket_opened' then 1 end) as support_tickets_14d
    from {{ ref('stg_events') }}
    where event_date >= current_date - interval '14 days'
    group by 1
),
sub_history as (
    select user_id, count(*) as subscription_changes,
        count(case when change_type = 'downgrade' then 1 end) as downgrades
    from {{ ref('stg_subscriptions') }} group by 1
)
select
    l.user_id,
    l.days_inactive, l.total_events, l.total_active_days, l.active_weeks,
    coalesce(h.health_score, 0) as health_score,
    coalesce(h.sessions_last_30d, 0) as sessions_last_30d,
    coalesce(e.events_last_14d, 0) as events_last_14d,
    coalesce(e.unique_events_14d, 0) as unique_events_14d,
    coalesce(e.support_tickets_14d, 0) as support_tickets_14d,
    coalesce(s.subscription_changes, 0) as subscription_changes,
    coalesce(s.downgrades, 0) as downgrades,
    l.lifecycle_state,
    case when l.lifecycle_state = 'churned' then 1 else 0 end as is_churned
from lifecycle l
left join health h on l.user_id = h.user_id
left join events_recent e on l.user_id = e.user_id
left join sub_history s on l.user_id = s.user_id
