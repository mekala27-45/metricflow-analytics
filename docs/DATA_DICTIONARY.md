# Data Dictionary

## Source Tables

### users
| Column | Type | Description |
|--------|------|-------------|
| user_id | string | Unique user identifier |
| email | string | User email |
| name | string | Full name |
| signup_date | date | Registration date |
| country | string | Country code |
| acquisition_channel | string | How user was acquired |
| initial_plan | string | First subscription plan |
| company_size | string | Company size bucket |
| industry | string | Company industry |

### events
| Column | Type | Description |
|--------|------|-------------|
| event_id | string | Unique event identifier |
| user_id | string | Foreign key to users |
| event_type | string | Type of product event |
| event_timestamp | timestamp | When event occurred |
| session_id | string | Session group identifier |
| platform | string | web, mobile_ios, mobile_android, api |

### subscriptions
| Column | Type | Description |
|--------|------|-------------|
| subscription_id | string | Unique subscription identifier |
| user_id | string | Foreign key to users |
| plan | string | Plan name (free/starter/professional/enterprise) |
| price | decimal | Monthly price |
| started_at | date | Subscription start |
| ended_at | date | Subscription end (null if active) |
| change_type | string | active/churned/upgrade/downgrade/renewed |

### payments
| Column | Type | Description |
|--------|------|-------------|
| payment_id | string | Unique payment identifier |
| user_id | string | Foreign key to users |
| amount | decimal | Payment amount |
| payment_date | date | Payment date |
| payment_status | string | succeeded/failed/refunded |

## Key Metrics

| Metric | Definition | Source |
|--------|-----------|--------|
| MRR | Sum of recognized monthly recurring revenue | fct_mrr |
| DAU | Distinct users with events on a given day | fct_daily_active_users |
| Activation Rate | % of users completing 5+ events | fct_activation |
| Health Score | Composite 0-100 score based on recency + frequency | fct_health_scores |
| North Star (WAU Core) | 7-day rolling average of core action users | north_star_metrics |
