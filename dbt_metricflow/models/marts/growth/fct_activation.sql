-- Activation funnel metrics
with lifecycle as (select * from {{ ref('int_user_lifecycle') }})
select
    date_trunc('month', signup_date) as cohort_month,
    acquisition_channel,
    count(*) as total_users,
    count(case when is_activated then 1 end) as activated_users,
    round(count(case when is_activated then 1 end)::decimal / nullif(count(*), 0) * 100, 1) as activation_rate,
    count(case when lifecycle_state = 'power_user' then 1 end) as power_users,
    count(case when lifecycle_state = 'churned' then 1 end) as churned_users,
    avg(total_events) as avg_events_per_user
from lifecycle group by 1, 2
