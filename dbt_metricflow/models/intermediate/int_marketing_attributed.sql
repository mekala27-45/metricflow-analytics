-- Multi-touch attribution
with touches as (select * from {{ ref('stg_marketing_touches') }}),
users as (select user_id, signup_date from {{ ref('stg_users') }})
select t.*,
    row_number() over (partition by t.user_id order by t.touch_timestamp asc) as touch_order_asc,
    row_number() over (partition by t.user_id order by t.touch_timestamp desc) as touch_order_desc,
    count(*) over (partition by t.user_id) as total_touches,
    1.0 / count(*) over (partition by t.user_id) as linear_weight,
    case when row_number() over (partition by t.user_id order by t.touch_timestamp asc) = 1 then 1.0 else 0.0 end as first_touch_weight,
    case when row_number() over (partition by t.user_id order by t.touch_timestamp desc) = 1 then 1.0 else 0.0 end as last_touch_weight
from touches t
left join users u on t.user_id = u.user_id
where t.touch_date <= u.signup_date
