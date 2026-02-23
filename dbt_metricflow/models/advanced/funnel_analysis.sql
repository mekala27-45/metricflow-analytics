-- Conversion funnel analysis
with signup_users as (
    select user_id, signup_date from {{ ref('stg_users') }}
),
first_events as (
    select user_id, event_type, min(event_date) as first_occurrence
    from {{ ref('stg_events') }} group by 1, 2
),
funnel as (
    select
        u.user_id,
        u.signup_date,
        1 as step_1_signup,
        case when exists(select 1 from first_events fe where fe.user_id = u.user_id and fe.event_type = 'page_view') then 1 else 0 end as step_2_first_visit,
        case when exists(select 1 from first_events fe where fe.user_id = u.user_id and fe.event_type = 'dashboard_viewed') then 1 else 0 end as step_3_view_dashboard,
        case when exists(select 1 from first_events fe where fe.user_id = u.user_id and fe.event_type = 'report_created') then 1 else 0 end as step_4_create_report,
        case when exists(select 1 from first_events fe where fe.user_id = u.user_id and fe.event_type = 'invite_sent') then 1 else 0 end as step_5_invite_team,
        case when exists(select 1 from {{ ref('stg_payments') }} p where p.user_id = u.user_id and p.payment_status = 'succeeded') then 1 else 0 end as step_6_paid_conversion
    from signup_users u
)
select
    date_trunc('month', signup_date) as cohort_month,
    count(*) as step_1_signups,
    sum(step_2_first_visit) as step_2_first_visit,
    sum(step_3_view_dashboard) as step_3_view_dashboard,
    sum(step_4_create_report) as step_4_create_report,
    sum(step_5_invite_team) as step_5_invite_team,
    sum(step_6_paid_conversion) as step_6_paid_conversion,
    round(sum(step_6_paid_conversion)::decimal / nullif(count(*), 0) * 100, 2) as overall_conversion_pct
from funnel group by 1
