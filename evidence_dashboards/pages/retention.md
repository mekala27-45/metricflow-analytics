---
title: Retention Intelligence
---

# 📊 Retention Intelligence

## Cohort Retention Heatmap

```sql cohort_data
select
    cohort_month,
    months_since_signup,
    retention_rate,
    active_users
from advanced.cohort_retention
where months_since_signup <= 12
order by cohort_month, months_since_signup
```

<DataTable data={cohort_data} rows=20>
    <Column id=cohort_month title="Cohort" />
    <Column id=months_since_signup title="Month #" />
    <Column id=retention_rate title="Retention %" fmt="pct1" />
    <Column id=active_users title="Active Users" />
</DataTable>

## User Lifecycle Distribution

```sql lifecycle
select
    lifecycle_state,
    count(*) as users
from intermediate.int_user_lifecycle
group by 1
order by users desc
```

<BarChart
    data={lifecycle}
    x=lifecycle_state
    y=users
    title="Current Lifecycle State Distribution"
/>

## Subscription Survival Curves

```sql survival
select
    plan_name,
    month_bucket,
    survival_rate_in_bucket,
    total_at_risk
from advanced.subscription_survival
where month_bucket <= 18
order by plan_name, month_bucket
```

<LineChart
    data={survival}
    x=month_bucket
    y=survival_rate_in_bucket
    series=plan_name
    title="Subscription Survival Rate by Plan"
    xAxisTitle="Months"
    yAxisTitle="Survival Rate"
/>
