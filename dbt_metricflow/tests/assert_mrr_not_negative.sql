-- Custom data test: MRR should never be negative
select payment_month, sum(mrr) as total_mrr
from {{ ref('fct_mrr') }}
group by 1
having sum(mrr) < 0
