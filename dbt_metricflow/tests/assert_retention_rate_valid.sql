-- Retention rate must be between 0 and 1
select cohort_month, months_since_signup, retention_rate
from {{ ref('cohort_retention') }}
where retention_rate < 0 or retention_rate > 1
