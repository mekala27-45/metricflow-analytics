-- Marketing channel ROI
with attr as (select * from {{ ref('int_marketing_attributed') }}),
users as (select user_id, initial_plan from {{ ref('stg_users') }}),
plans as (select * from {{ ref('plan_details') }})
select
    a.channel,
    date_trunc('month', a.touch_date) as month,
    count(distinct a.user_id) as touches,
    count(distinct case when a.is_converting_touch then a.user_id end) as conversions,
    sum(a.cost) as total_spend,
    round(sum(a.cost) / nullif(count(distinct case when a.is_converting_touch then a.user_id end), 0), 2) as cac,
    count(distinct case when a.is_converting_touch and u.initial_plan != 'free' then a.user_id end) as paid_conversions
from attr a
left join users u on a.user_id = u.user_id
group by 1, 2
