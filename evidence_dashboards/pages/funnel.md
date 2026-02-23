---
title: Funnel Diagnostics
---

# 🔄 Funnel Diagnostics

## Conversion Funnel

```sql funnel
select
    cohort_month,
    step_1_signups,
    step_2_first_visit,
    step_3_view_dashboard,
    step_4_create_report,
    step_5_invite_team,
    step_6_paid_conversion,
    overall_conversion_pct
from advanced.funnel_analysis
order by cohort_month desc
limit 12
```

<DataTable data={funnel} rows=12>
    <Column id=cohort_month title="Cohort" />
    <Column id=step_1_signups title="1. Signup" />
    <Column id=step_2_first_visit title="2. Visit" />
    <Column id=step_3_view_dashboard title="3. Dashboard" />
    <Column id=step_4_create_report title="4. Report" />
    <Column id=step_5_invite_team title="5. Invite" />
    <Column id=step_6_paid_conversion title="6. Paid" />
    <Column id=overall_conversion_pct title="Conv %" fmt="pct1" />
</DataTable>

## Activation by Channel

```sql activation
select
    cohort_month,
    acquisition_channel,
    total_users,
    activated_users,
    activation_rate
from marts.fct_activation
order by cohort_month desc, activation_rate desc
```

<BarChart
    data={activation}
    x=acquisition_channel
    y=activation_rate
    title="Activation Rate by Acquisition Channel"
    yAxisTitle="Activation %"
/>
