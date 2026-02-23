-- stg_payments.sql — Payment transactions
with source as (
    select * from {{ source('raw', 'payments') }}
),

cleaned as (
    select
        payment_id,
        subscription_id,
        user_id,
        cast(amount as decimal(10,2)) as amount,
        upper(trim(currency)) as currency,
        cast(payment_date as date) as payment_date,
        date_trunc('month', cast(payment_date as date)) as payment_month,
        lower(trim(payment_method)) as payment_method,
        lower(trim(status)) as payment_status,
        lower(trim(billing_interval)) as billing_interval,
        -- normalized MRR contribution
        case
            when lower(trim(billing_interval)) = 'annual'
            then round(cast(amount as decimal(10,2)) / 12, 2)
            else cast(amount as decimal(10,2))
        end as mrr_contribution
    from source
    where payment_id is not null
      and cast(amount as decimal(10,2)) >= 0
)

select * from cleaned
