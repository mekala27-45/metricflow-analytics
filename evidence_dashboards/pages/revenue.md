---
title: Revenue Deep-Dive
---

# 💰 Revenue Deep-Dive

## Monthly Recurring Revenue by Plan

```sql mrr_trend
select
    payment_month,
    plan_name,
    mrr,
    paying_customers,
    arpu
from marts.fct_mrr
order by payment_month, plan_name
```

<BarChart
    data={mrr_trend}
    x=payment_month
    y=mrr
    series=plan_name
    type=stacked
    title="MRR by Plan"
    yAxisTitle="MRR ($)"
/>

## MRR Waterfall

```sql waterfall
select
    month,
    new_mrr,
    expansion_mrr,
    -contraction_mrr as contraction_mrr,
    -churned_mrr as churned_mrr,
    new_mrr + expansion_mrr - contraction_mrr - churned_mrr as net_mrr_change
from marts.fct_mrr_waterfall
order by month
```

<BarChart
    data={waterfall}
    x=month
    y={["new_mrr", "expansion_mrr", "contraction_mrr", "churned_mrr"]}
    type=stacked
    title="MRR Waterfall (Monthly)"
/>

## Revenue Anomalies

```sql anomalies
select * from advanced.anomaly_flags
where is_anomaly = true
order by payment_date desc
limit 20
```

<DataTable data={anomalies} rows=10>
    <Column id=payment_date title="Date" />
    <Column id=daily_revenue title="Revenue" fmt="usd" />
    <Column id=z_score title="Z-Score" fmt="num2" />
    <Column id=anomaly_type title="Type" />
</DataTable>
