-- DAU/WAU/MAU metrics
with events as (select * from {{ ref('int_events_enriched') }})
select
    event_date,
    count(distinct user_id) as dau,
    count(distinct case when is_high_value_action then user_id end) as dau_core,
    count(*) as total_events,
    count(distinct case when feature_category = 'core_analytics' then user_id end) as analytics_users,
    count(distinct case when feature_category = 'platform' then user_id end) as platform_users,
    count(distinct case when feature_category = 'collaboration' then user_id end) as collab_users
from events group by 1
