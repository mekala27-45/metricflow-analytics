-- stg_marketing_touches.sql — Marketing attribution
with source as (
    select * from {{ source('raw', 'marketing_touches') }}
),

cleaned as (
    select
        touch_id,
        user_id,
        trim(campaign) as campaign,
        lower(trim(channel)) as channel,
        cast(touch_timestamp as timestamp) as touch_timestamp,
        cast(touch_timestamp as date) as touch_date,
        lower(trim(touch_type)) as touch_type,
        coalesce(is_converting_touch, false) as is_converting_touch,
        cast(coalesce(cost, 0) as decimal(10,2)) as cost
    from source
    where touch_id is not null
)

select * from cleaned
