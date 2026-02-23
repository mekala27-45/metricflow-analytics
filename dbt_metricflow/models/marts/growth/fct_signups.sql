-- Daily signup metrics
with users as (select * from {{ ref('stg_users') }})
select
    signup_date, signup_cohort_month, acquisition_channel, initial_plan, country, company_size,
    count(*) as signups,
    count(case when is_verified then 1 end) as verified_signups
from users group by 1, 2, 3, 4, 5, 6
