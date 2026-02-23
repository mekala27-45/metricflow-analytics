-- Revenue anomaly detection flags
with daily_revenue as (
    select payment_date, sum(recognized_mrr) as daily_revenue, count(distinct user_id) as paying_users
    from {{ ref('int_revenue_normalized') }} where payment_status = 'succeeded' group by 1
),
with_stats as (
    select *,
        avg(daily_revenue) over (order by payment_date rows between 30 preceding and 1 preceding) as rolling_avg,
        stddev(daily_revenue) over (order by payment_date rows between 30 preceding and 1 preceding) as rolling_std
    from daily_revenue
)
select *,
    case
        when rolling_std > 0 then (daily_revenue - rolling_avg) / rolling_std
        else 0
    end as z_score,
    case
        when rolling_std > 0 and abs((daily_revenue - rolling_avg) / rolling_std) > 2.5 then true
        else false
    end as is_anomaly,
    case
        when rolling_std > 0 and (daily_revenue - rolling_avg) / rolling_std > 2.5 then 'revenue_spike'
        when rolling_std > 0 and (daily_revenue - rolling_avg) / rolling_std < -2.5 then 'revenue_drop'
        else 'normal'
    end as anomaly_type
from with_stats
