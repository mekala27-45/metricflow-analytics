---
title: MetricFlow — Executive Dashboard
---

# 🔮 MetricFlow Analytics

Welcome to the MetricFlow analytics platform. This dashboard provides real-time
intelligence across growth, revenue, retention, and product metrics.

## North Star Metrics

```sql north_star
select
    metric_date,
    dau,
    core_action_users,
    daily_mrr,
    new_signups,
    north_star_wau_core
from advanced.north_star_metrics
where metric_date >= current_date - interval '90 days'
order by metric_date
```

<LineChart
    data={north_star}
    x=metric_date
    y={["dau", "core_action_users"]}
    title="Daily Active Users & Core Action Users"
    yAxisTitle="Users"
/>

<LineChart
    data={north_star}
    x=metric_date
    y=daily_mrr
    title="Daily MRR Trend"
    yAxisTitle="Revenue ($)"
/>

## Quick Links

- [📊 Retention Intelligence](/retention)
- [💰 Revenue Deep-Dive](/revenue)
- [🔄 Funnel Diagnostics](/funnel)
- [📦 Product Adoption](/product)
- [🚨 Churn Early Warning](/churn)
