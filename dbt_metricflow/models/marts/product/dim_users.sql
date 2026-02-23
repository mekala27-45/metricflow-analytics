-- Enriched user dimension
with lifecycle as (select * from {{ ref('int_user_lifecycle') }}),
users as (select * from {{ ref('stg_users') }}),
revenue as (
    select user_id, sum(recognized_mrr) as total_revenue, count(*) as payment_count,
        min(payment_date) as first_payment, max(payment_date) as last_payment
    from {{ ref('int_revenue_normalized') }} where payment_status = 'succeeded' group by 1
)
select
    u.user_id, u.email, u.full_name, u.signup_date, u.signup_cohort_month,
    u.country, u.acquisition_channel, u.company_size, u.industry, u.is_verified,
    l.lifecycle_state, l.is_activated, l.current_plan, l.has_active_sub,
    l.total_events, l.total_active_days, l.days_inactive, l.active_weeks,
    coalesce(r.total_revenue, 0) as lifetime_revenue,
    coalesce(r.payment_count, 0) as total_payments,
    r.first_payment, r.last_payment,
    -- Engagement score (0-100)
    least(100, (
        coalesce(l.total_active_days, 0) * 0.5 +
        least(coalesce(l.total_events, 0), 500) * 0.05 +
        coalesce(l.active_weeks, 0) * 2
    )::int) as engagement_score
from users u
left join lifecycle l on u.user_id = l.user_id
left join revenue r on u.user_id = r.user_id
