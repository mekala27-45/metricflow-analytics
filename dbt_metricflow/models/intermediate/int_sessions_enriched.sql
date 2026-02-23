-- int_sessions_enriched.sql — Sessions joined with user context
with sessions as (
    select * from {{ ref('stg_sessions') }}
),

users as (
    select user_id, signup_date, acquisition_channel, initial_plan
    from {{ ref('stg_users') }}
),

enriched as (
    select
        s.*,
        u.signup_date,
        u.acquisition_channel,
        u.initial_plan,
        s.session_date - u.signup_date as days_since_signup,
        case
            when s.session_date - u.signup_date <= 7 then 'week_1'
            when s.session_date - u.signup_date <= 30 then 'month_1'
            when s.session_date - u.signup_date <= 90 then 'quarter_1'
            else 'mature'
        end as lifecycle_stage
    from sessions s
    left join users u on s.user_id = u.user_id
)

select * from enriched
