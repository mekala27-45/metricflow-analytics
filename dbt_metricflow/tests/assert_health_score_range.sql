-- Health score must be 0-100
select user_id, health_score
from {{ ref('fct_health_scores') }}
where health_score < 0 or health_score > 100
