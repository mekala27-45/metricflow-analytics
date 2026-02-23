-- stg_subscriptions.sql — Subscription lifecycle
with source as (
    select * from {{ source('raw', 'subscriptions') }}
),

cleaned as (
    select
        subscription_id,
        user_id,
        lower(trim(plan)) as plan_name,
        cast(price as decimal(10,2)) as monthly_price,
        cast(started_at as date) as started_at,
        cast(ended_at as date) as ended_at,
        lower(trim(change_type)) as change_type,
        lower(trim(billing_interval)) as billing_interval,
        -- derived
        case
            when ended_at is null then current_date - cast(started_at as date)
            else cast(ended_at as date) - cast(started_at as date)
        end as duration_days,
        ended_at is null as is_active
    from source
    where subscription_id is not null
)

select * from cleaned
