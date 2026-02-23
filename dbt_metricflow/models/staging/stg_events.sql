-- stg_events.sql — Cleaned product events
with source as (
    select * from {{ source('raw', 'events') }}
),

cleaned as (
    select
        event_id,
        user_id,
        lower(trim(event_type)) as event_type,
        cast(event_timestamp as timestamp) as event_timestamp,
        cast(event_timestamp as date) as event_date,
        session_id,
        lower(trim(platform)) as platform,
        trim(page_url) as page_url,
        -- time dimensions
        extract(hour from cast(event_timestamp as timestamp)) as event_hour,
        extract(dow from cast(event_timestamp as timestamp)) as event_day_of_week,
        date_trunc('month', cast(event_timestamp as date)) as event_month
    from source
    where event_id is not null
      and user_id is not null
)

select * from cleaned
