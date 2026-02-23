-- Customer LTV feature table for ML
with users as (select * from {{ ref('dim_users') }}),
activity as (
    select user_id,
        count(distinct event_date) as active_days_first_30,
        count(*) as events_first_30,
        count(distinct event_type) as unique_features_first_30
    from {{ ref('stg_events') }}
    where event_date <= (select signup_date + 30 from {{ ref('stg_users') }} su where su.user_id = {{ ref('stg_events') }}.user_id limit 1)
    group by 1
),
sessions as (
    select user_id, avg(duration_seconds) as avg_session_secs, count(*) as total_sessions
    from {{ ref('stg_sessions') }} group by 1
)
select
    u.user_id, u.signup_date, u.acquisition_channel, u.company_size, u.industry,
    u.lifetime_revenue, u.total_payments, u.engagement_score,
    u.lifecycle_state, u.current_plan,
    coalesce(a.active_days_first_30, 0) as active_days_first_30,
    coalesce(a.events_first_30, 0) as events_first_30,
    coalesce(a.unique_features_first_30, 0) as unique_features_first_30,
    coalesce(s.avg_session_secs, 0) as avg_session_seconds,
    coalesce(s.total_sessions, 0) as total_sessions
from users u
left join activity a on u.user_id = a.user_id
left join sessions s on u.user_id = s.user_id
