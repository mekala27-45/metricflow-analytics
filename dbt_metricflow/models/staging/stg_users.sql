-- stg_users.sql — Clean user records with consistent typing
with source as (
    select * from {{ source('raw', 'users') }}
),

cleaned as (
    select
        user_id,
        lower(trim(email)) as email,
        trim(name) as full_name,
        cast(signup_date as date) as signup_date,
        cast(signup_timestamp as timestamp) as signup_timestamp,
        upper(trim(country)) as country,
        lower(trim(acquisition_channel)) as acquisition_channel,
        lower(trim(initial_plan)) as initial_plan,
        trim(company_size) as company_size,
        trim(industry) as industry,
        coalesce(is_verified, false) as is_verified,
        -- derived
        extract(year from signup_date) as signup_year,
        extract(month from signup_date) as signup_month,
        date_trunc('month', cast(signup_date as date)) as signup_cohort_month,
        date_trunc('week', cast(signup_date as date)) as signup_cohort_week
    from source
    where user_id is not null
)

select * from cleaned
