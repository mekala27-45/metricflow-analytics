-- Monthly Recurring Revenue
with revenue as (select * from {{ ref('int_revenue_normalized') }} where payment_status = 'succeeded')
select
    payment_month,
    plan_name,
    sum(recognized_mrr) as mrr,
    count(distinct user_id) as paying_customers,
    avg(recognized_mrr) as arpu,
    sum(case when billing_interval = 'annual' then recognized_mrr else 0 end) as annual_mrr,
    sum(case when billing_interval = 'monthly' then recognized_mrr else 0 end) as monthly_mrr
from revenue group by 1, 2
