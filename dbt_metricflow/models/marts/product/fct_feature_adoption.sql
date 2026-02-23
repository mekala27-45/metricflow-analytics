-- Feature adoption tracking
with events as (select * from {{ ref('int_events_enriched') }})
select
    date_trunc('week', event_date) as week,
    event_type as feature,
    feature_category,
    count(distinct user_id) as unique_users,
    count(*) as total_usage,
    avg(days_since_signup) as avg_days_to_adopt,
    count(distinct case when days_since_signup <= 7 then user_id end) as first_week_adopters,
    count(distinct case when days_since_signup <= 30 then user_id end) as first_month_adopters
from events group by 1, 2, 3
