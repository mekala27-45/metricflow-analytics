-- Customer health scoring
with lifecycle as (select * from {{ ref('int_user_lifecycle') }}),
sessions as (
    select user_id, avg(duration_seconds) as avg_session_duration,
        count(*) as session_count_30d
    from {{ ref('stg_sessions') }}
    where session_date >= current_date - interval '30 days'
    group by 1
)
select
    l.user_id, l.lifecycle_state, l.current_plan, l.total_events, l.days_inactive,
    coalesce(s.avg_session_duration, 0) as avg_session_duration_30d,
    coalesce(s.session_count_30d, 0) as sessions_last_30d,
    -- Composite health score (0-100)
    least(100, greatest(0,
        (case when l.days_inactive < 3 then 40 when l.days_inactive < 7 then 30
              when l.days_inactive < 14 then 15 when l.days_inactive < 30 then 5 else 0 end) +
        least(30, coalesce(s.session_count_30d, 0) * 2) +
        least(30, coalesce(l.total_events, 0) * 0.1)
    ))::int as health_score,
    case
        when l.days_inactive > 21 then 'critical'
        when l.days_inactive > 14 then 'warning'
        when l.days_inactive > 7 then 'monitor'
        else 'healthy'
    end as health_status
from lifecycle l left join sessions s on l.user_id = s.user_id
