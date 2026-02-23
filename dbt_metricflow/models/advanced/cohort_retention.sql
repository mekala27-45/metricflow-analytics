-- Cohort retention analysis (month-over-month)
with user_cohorts as (
    select user_id, signup_cohort_month as cohort_month
    from {{ ref('stg_users') }}
),
monthly_activity as (
    select distinct user_id, date_trunc('month', event_date) as activity_month
    from {{ ref('stg_events') }}
)
select
    c.cohort_month,
    a.activity_month,
    (extract(year from a.activity_month) - extract(year from c.cohort_month)) * 12 +
    (extract(month from a.activity_month) - extract(month from c.cohort_month)) as months_since_signup,
    count(distinct a.user_id) as active_users,
    count(distinct a.user_id)::decimal / nullif(
        (select count(distinct uc2.user_id) from user_cohorts uc2 where uc2.cohort_month = c.cohort_month), 0
    ) as retention_rate
from user_cohorts c
inner join monthly_activity a on c.user_id = a.user_id and a.activity_month >= c.cohort_month
group by 1, 2
