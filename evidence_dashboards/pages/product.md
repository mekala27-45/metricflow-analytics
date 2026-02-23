---
title: Product Adoption
---

# 📦 Product Adoption

## Daily Active Users

```sql dau
select
    event_date,
    dau,
    dau_core,
    analytics_users,
    platform_users
from marts.fct_daily_active_users
where event_date >= current_date - interval '90 days'
order by event_date
```

<LineChart
    data={dau}
    x=event_date
    y={["dau", "dau_core"]}
    title="DAU vs Core DAU (Last 90 Days)"
/>

## Feature Adoption

```sql features
select
    feature,
    feature_category,
    sum(unique_users) as total_users,
    sum(total_usage) as total_events
from marts.fct_feature_adoption
group by 1, 2
order by total_users desc
```

<BarChart
    data={features}
    x=feature
    y=total_users
    title="Feature Adoption by Unique Users"
    xAxisTitle="Feature"
    yAxisTitle="Unique Users"
/>

## RFM Customer Segments

```sql rfm
select
    rfm_segment,
    count(*) as users,
    round(avg(monetary), 2) as avg_revenue,
    round(avg(recency_days), 0) as avg_recency_days
from advanced.rfm_segmentation
group by 1
order by avg_revenue desc
```

<DataTable data={rfm}>
    <Column id=rfm_segment title="Segment" />
    <Column id=users title="Users" />
    <Column id=avg_revenue title="Avg Revenue" fmt="usd" />
    <Column id=avg_recency_days title="Avg Days Since Active" />
</DataTable>
