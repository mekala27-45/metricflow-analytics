-- User lifecycle state classification
with users as (select * from {{ ref('stg_users') }}),
latest_sub as (
    select user_id, plan_name as current_plan, is_active as has_active_sub,
        row_number() over (partition by user_id order by started_at desc) as rn
    from {{ ref('stg_subscriptions') }}
),
activity as (
    select user_id, max(event_date) as last_active_date,
        count(distinct event_date) as total_active_days, count(*) as total_events,
        count(distinct date_trunc('week', event_date)) as active_weeks
    from {{ ref('stg_events') }} group by 1
)
select
    u.user_id, u.signup_date, u.acquisition_channel, u.company_size, u.industry,
    ls.current_plan, ls.has_active_sub,
    a.last_active_date, a.total_active_days, coalesce(a.total_events, 0) as total_events, a.active_weeks,
    current_date - coalesce(a.last_active_date, u.signup_date) as days_inactive,
    case
        when current_date - u.signup_date <= 7 then 'new'
        when a.total_events is null or a.total_events < {{ var('activation_threshold_events') }} then 'inactive'
        when current_date - coalesce(a.last_active_date, u.signup_date) > {{ var('churn_lookback_days') }} then 'churned'
        when current_date - coalesce(a.last_active_date, u.signup_date) > 14 then 'at_risk'
        when coalesce(a.total_events, 0) >= {{ var('power_user_threshold_events') }} and a.active_weeks >= 8 then 'power_user'
        when a.total_active_days >= 10 then 'active'
        else 'casual'
    end as lifecycle_state,
    coalesce(a.total_events, 0) >= {{ var('activation_threshold_events') }} as is_activated
from users u
left join latest_sub ls on u.user_id = ls.user_id and ls.rn = 1
left join activity a on u.user_id = a.user_id
