-- Events with user + feature context
with events as (select * from {{ ref('stg_events') }}),
users as (select user_id, signup_date, initial_plan from {{ ref('stg_users') }})
select
    e.*, u.signup_date, u.initial_plan,
    e.event_date - u.signup_date as days_since_signup,
    case
        when e.event_type in ('report_created','dashboard_viewed','export_data') then 'core_analytics'
        when e.event_type in ('api_call','integration_connected') then 'platform'
        when e.event_type in ('invite_sent','comment_added') then 'collaboration'
        when e.event_type in ('alert_configured','settings_changed') then 'configuration'
        when e.event_type = 'support_ticket_opened' then 'support'
        else 'navigation'
    end as feature_category,
    e.event_type in ('report_created','api_call','integration_connected',
                     'export_data','alert_configured','invite_sent') as is_high_value_action
from events e
left join users u on e.user_id = u.user_id
