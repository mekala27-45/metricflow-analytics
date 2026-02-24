-- A/B test simulation results
with users as (select * from {{ ref('stg_users') }}),
events as (
    select user_id, count(*) as events_7d
    from {{ ref('stg_events') }}
    where event_date >= (select max(event_date) - 7 from {{ ref('stg_events') }})
    group by 1
)
select
    -- Simulate experiment: odd user_ids = variant, even = control
    -- Using hash() (DuckDB native) instead of hashtext() (PostgreSQL only)
    case when abs(hash(u.user_id)::bigint) % 2 = 0 then 'control' else 'variant' end as experiment_group,
    u.initial_plan,
    count(*) as users,
    avg(coalesce(e.events_7d, 0)) as avg_events,
    stddev(coalesce(e.events_7d, 0)) as stddev_events,
    count(case when coalesce(e.events_7d, 0) > 0 then 1 end)::decimal / nullif(count(*), 0) as activation_rate
from users u left join events e on u.user_id = e.user_id
group by 1, 2
