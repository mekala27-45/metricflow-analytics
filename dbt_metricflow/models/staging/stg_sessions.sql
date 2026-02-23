-- stg_sessions.sql — Session-level engagement
with source as (
    select * from {{ source('raw', 'sessions') }}
),

cleaned as (
    select
        session_id_unique as session_id,
        user_id,
        session_id as session_group_id,
        cast(session_start as timestamp) as session_start,
        cast(session_end as timestamp) as session_end,
        cast(session_start as date) as session_date,
        cast(event_count as integer) as event_count,
        lower(trim(platform)) as platform,
        cast(duration_seconds as integer) as duration_seconds,
        -- engagement classification
        case
            when cast(duration_seconds as integer) < 30 then 'bounce'
            when cast(duration_seconds as integer) < 300 then 'short'
            when cast(duration_seconds as integer) < 1800 then 'medium'
            else 'deep'
        end as engagement_level
    from source
    where session_id_unique is not null
)

select * from cleaned
