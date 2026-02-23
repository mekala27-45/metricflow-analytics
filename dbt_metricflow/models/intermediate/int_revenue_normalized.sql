-- int_revenue_normalized.sql — Revenue with MRR normalization
with payments as (
    select * from {{ ref('stg_payments') }}
),
subscriptions as (
    select subscription_id, plan_name from {{ ref('stg_subscriptions') }}
),
normalized as (
    select
        p.payment_id, p.user_id, p.subscription_id, s.plan_name,
        p.amount as gross_amount, p.payment_status, p.payment_date, p.payment_month,
        p.billing_interval, p.mrr_contribution,
        case when p.payment_status = 'succeeded' then p.mrr_contribution else 0 end as recognized_mrr,
        case when p.payment_status = 'refunded' then p.amount else 0 end as refunded_amount,
        case when p.payment_status = 'failed' then p.amount else 0 end as failed_amount
    from payments p
    left join subscriptions s on p.subscription_id = s.subscription_id
)
select * from normalized
