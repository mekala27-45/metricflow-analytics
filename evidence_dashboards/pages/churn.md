---
title: Churn Early Warning
---

# 🚨 Churn Early Warning

## Customer Health Distribution

```sql health
select
    health_status,
    count(*) as users,
    round(avg(health_score), 1) as avg_health_score
from marts.fct_health_scores
group by 1
order by
    case health_status
        when 'critical' then 1
        when 'warning' then 2
        when 'monitor' then 3
        when 'healthy' then 4
    end
```

<BarChart
    data={health}
    x=health_status
    y=users
    title="Customer Health Distribution"
    colorPalette={["#d32f2f", "#f57c00", "#fbc02d", "#388e3c"]}
/>

## At-Risk Users

```sql at_risk
select
    user_id,
    lifecycle_state,
    current_plan,
    health_score,
    days_inactive,
    sessions_last_30d
from marts.fct_health_scores
where health_status in ('critical', 'warning')
order by health_score asc
limit 25
```

<DataTable data={at_risk} rows=15>
    <Column id=user_id title="User ID" />
    <Column id=current_plan title="Plan" />
    <Column id=health_score title="Health Score" />
    <Column id=days_inactive title="Days Inactive" />
    <Column id=sessions_last_30d title="Sessions (30d)" />
</DataTable>

## Churn Risk Factors

The churn prediction model identifies these as the top risk indicators:

1. **Days Inactive** — Most predictive single feature
2. **Sessions Last 30 Days** — Low session count = high risk
3. **Health Score** — Composite metric capturing overall engagement
4. **Support Tickets** — Rising tickets without resolution
5. **Subscription Downgrades** — History of plan downgrades
