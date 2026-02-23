-- Payment fact table
select
    payment_id, user_id, subscription_id, plan_name,
    gross_amount, recognized_mrr, refunded_amount, failed_amount,
    payment_status, payment_date, payment_month, billing_interval
from {{ ref('int_revenue_normalized') }}
