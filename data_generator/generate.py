"""
MetricFlow — Synthetic SaaS Data Generator
Generates realistic, correlated data for 50K+ users across 4 years.
Produces Parquet files optimized for DuckDB analytics.
"""

from __future__ import annotations

import hashlib
import os
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from data_generator.config import GeneratorConfig

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)
console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _id() -> str:
    return uuid.uuid4().hex[:12]


def _date_range(start: date, end: date) -> list[date]:
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def _seasonal_weight(d: date) -> float:
    """Simulate seasonal signup patterns: peaks in Jan, Sep."""
    month_weights = {1: 1.4, 2: 1.1, 3: 1.0, 4: 0.9, 5: 0.85, 6: 0.8,
                     7: 0.75, 8: 0.9, 9: 1.3, 10: 1.1, 11: 1.0, 12: 0.7}
    return month_weights.get(d.month, 1.0)


def _growth_curve(d: date, start: date) -> float:
    """Exponential growth with saturation."""
    months = (d.year - start.year) * 12 + (d.month - start.month)
    return 1.0 + 0.8 * np.log1p(months)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def generate_users(cfg: GeneratorConfig) -> pd.DataFrame:
    console.print("[bold blue]Generating users...[/]")
    dates = _date_range(cfg.history_start, cfg.history_end)
    
    # Distribute signups across dates with seasonality + growth
    weights = np.array([_seasonal_weight(d) * _growth_curve(d, cfg.history_start) for d in dates])
    weights /= weights.sum()
    signup_dates = np.random.choice(dates, size=cfg.num_users, p=weights)
    
    countries = ["US"] * 50 + ["UK"] * 12 + ["CA"] * 10 + ["DE"] * 8 + ["FR"] * 5 + \
                ["AU"] * 5 + ["IN"] * 4 + ["BR"] * 3 + ["JP"] * 2 + ["Other"] * 1
    
    plan_names = list(cfg.plans.keys())
    plan_weights = [cfg.plans[p]["weight"] for p in plan_names]
    
    users = []
    for i in range(cfg.num_users):
        user_id = _id()
        signup = signup_dates[i]
        channel = random.choice(cfg.acquisition_channels)
        plan = random.choices(plan_names, weights=plan_weights, k=1)[0]
        country = random.choice(countries)
        company_size = random.choices(
            ["1-10", "11-50", "51-200", "201-1000", "1000+"],
            weights=[0.35, 0.30, 0.20, 0.10, 0.05], k=1
        )[0]
        
        users.append({
            "user_id": user_id,
            "email": f"user_{user_id}@{fake.free_email_domain()}",
            "name": fake.name(),
            "signup_date": signup,
            "signup_timestamp": datetime.combine(signup, datetime.min.time().replace(
                hour=random.randint(6, 23), minute=random.randint(0, 59)
            )),
            "country": country,
            "acquisition_channel": channel,
            "initial_plan": plan,
            "company_size": company_size,
            "industry": random.choice([
                "Technology", "Finance", "Healthcare", "Education", "Retail",
                "Manufacturing", "Media", "Consulting", "Non-profit", "Government",
            ]),
            "is_verified": random.random() < 0.85,
        })
    
    return pd.DataFrame(users)


# ---------------------------------------------------------------------------
# Subscriptions (with upgrades, downgrades, churns)
# ---------------------------------------------------------------------------

def generate_subscriptions(users_df: pd.DataFrame, cfg: GeneratorConfig) -> pd.DataFrame:
    console.print("[bold blue]Generating subscriptions...[/]")
    plan_order = ["free", "starter", "professional", "enterprise"]
    churn_mult = {
        "free": cfg.churn_multiplier_free,
        "starter": cfg.churn_multiplier_starter,
        "professional": cfg.churn_multiplier_pro,
        "enterprise": cfg.churn_multiplier_enterprise,
    }
    
    subs = []
    for _, user in users_df.iterrows():
        current_plan = user["initial_plan"]
        current_start = user["signup_date"]
        user_id = user["user_id"]
        
        while current_start < cfg.history_end:
            # Duration on this plan (geometric distribution)
            monthly_churn = cfg.base_monthly_churn_rate * churn_mult[current_plan]
            months_on_plan = max(1, int(np.random.geometric(monthly_churn)))
            end_date = min(
                current_start + timedelta(days=months_on_plan * 30),
                cfg.history_end
            )
            
            # Determine what happens next
            rand = random.random()
            plan_idx = plan_order.index(current_plan)
            
            if end_date >= cfg.history_end:
                change_type = "active"
                next_plan = current_plan
            elif rand < 0.15 and plan_idx < 3:  # upgrade
                change_type = "upgrade"
                next_plan = plan_order[min(plan_idx + 1, 3)]
            elif rand < 0.22 and plan_idx > 0:  # downgrade
                change_type = "downgrade"
                next_plan = plan_order[max(plan_idx - 1, 0)]
            elif rand < 0.22 + monthly_churn * 3:  # churn
                change_type = "churned"
                next_plan = None
            else:
                change_type = "renewed"
                next_plan = current_plan
            
            subs.append({
                "subscription_id": _id(),
                "user_id": user_id,
                "plan": current_plan,
                "price": cfg.plans[current_plan]["price"],
                "started_at": current_start,
                "ended_at": end_date if change_type != "active" else None,
                "change_type": change_type,
                "billing_interval": "monthly" if random.random() < 0.7 else "annual",
            })
            
            if change_type == "churned":
                # Some users reactivate after churn
                if random.random() < 0.15:
                    reactivation_gap = random.randint(30, 180)
                    current_start = end_date + timedelta(days=reactivation_gap)
                    current_plan = random.choices(plan_names := list(cfg.plans.keys()),
                                                  weights=[0.2, 0.4, 0.3, 0.1], k=1)[0]
                else:
                    break
            elif change_type == "active":
                break
            else:
                current_start = end_date
                if next_plan:
                    current_plan = next_plan
    
    return pd.DataFrame(subs)


# ---------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------

def generate_payments(subs_df: pd.DataFrame, cfg: GeneratorConfig) -> pd.DataFrame:
    console.print("[bold blue]Generating payments...[/]")
    payments = []
    
    for _, sub in subs_df.iterrows():
        if sub["price"] == 0:
            continue
        
        start = sub["started_at"]
        end = sub["ended_at"] or cfg.history_end
        
        if sub["billing_interval"] == "annual":
            amount = sub["price"] * 12 * (0.85 if random.random() < 0.6 else 1.0)  # annual discount
            payments.append({
                "payment_id": _id(),
                "subscription_id": sub["subscription_id"],
                "user_id": sub["user_id"],
                "amount": round(amount, 2),
                "currency": "USD",
                "payment_date": start,
                "payment_method": random.choice(["credit_card", "debit_card", "paypal", "wire_transfer"]),
                "status": random.choices(["succeeded", "failed", "refunded"], weights=[0.94, 0.04, 0.02], k=1)[0],
                "billing_interval": "annual",
            })
        else:
            current = start
            while current < end:
                payments.append({
                    "payment_id": _id(),
                    "subscription_id": sub["subscription_id"],
                    "user_id": sub["user_id"],
                    "amount": round(sub["price"] * (1 + random.uniform(-0.02, 0.02)), 2),
                    "currency": "USD",
                    "payment_date": current,
                    "payment_method": random.choice(["credit_card", "debit_card", "paypal"]),
                    "status": random.choices(["succeeded", "failed", "refunded"], weights=[0.94, 0.04, 0.02], k=1)[0],
                    "billing_interval": "monthly",
                })
                current += timedelta(days=30)
    
    return pd.DataFrame(payments)


# ---------------------------------------------------------------------------
# Events (product usage)
# ---------------------------------------------------------------------------

def generate_events(users_df: pd.DataFrame, subs_df: pd.DataFrame, cfg: GeneratorConfig) -> pd.DataFrame:
    console.print("[bold blue]Generating events (this takes a moment)...[/]")
    
    # Build user activity periods
    user_periods = {}
    for _, sub in subs_df.iterrows():
        uid = sub["user_id"]
        start = sub["started_at"]
        end = sub["ended_at"] or cfg.history_end
        if uid not in user_periods:
            user_periods[uid] = []
        user_periods[uid].append((start, end, sub["plan"]))
    
    plan_activity = {"free": 2, "starter": 5, "professional": 12, "enterprise": 20}
    
    events = []
    for uid, periods in user_periods.items():
        for start, end, plan in periods:
            daily_events = plan_activity.get(plan, 3)
            days_active = (end - start).days
            
            # Sample active days (not every day)
            activity_rate = min(0.85, 0.3 + plan_activity[plan] * 0.03)
            num_active_days = max(1, int(days_active * activity_rate))
            
            if days_active <= 0:
                continue
            
            active_days = sorted(random.sample(
                range(days_active), min(num_active_days, days_active)
            ))
            
            for day_offset in active_days:
                event_date = start + timedelta(days=day_offset)
                n_events = max(1, int(np.random.poisson(daily_events)))
                
                for _ in range(min(n_events, 30)):  # cap per day
                    events.append({
                        "event_id": _id(),
                        "user_id": uid,
                        "event_type": random.choices(
                            cfg.event_types,
                            weights=[20, 15, 10, 12, 8, 5, 3, 2, 8, 4, 2, 3, 5, 3],
                            k=1
                        )[0],
                        "event_timestamp": datetime.combine(event_date, datetime.min.time().replace(
                            hour=random.choices(range(24), weights=[
                                1,1,1,1,1,2,4,8,12,14,14,12,10,12,14,13,11,9,7,5,4,3,2,1
                            ], k=1)[0],
                            minute=random.randint(0, 59),
                            second=random.randint(0, 59),
                        )),
                        "session_id": hashlib.md5(f"{uid}{event_date}{random.randint(0,3)}".encode()).hexdigest()[:12],
                        "platform": random.choices(["web", "mobile_ios", "mobile_android", "api"],
                                                    weights=[0.55, 0.20, 0.15, 0.10], k=1)[0],
                        "page_url": random.choice([
                            "/dashboard", "/reports", "/settings", "/billing",
                            "/integrations", "/team", "/analytics", "/api-docs",
                        ]) if random.random() < 0.7 else None,
                    })
    
    return pd.DataFrame(events)


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

def generate_sessions(events_df: pd.DataFrame) -> pd.DataFrame:
    console.print("[bold blue]Generating sessions...[/]")
    
    sessions = events_df.groupby(["user_id", "session_id"]).agg(
        session_start=("event_timestamp", "min"),
        session_end=("event_timestamp", "max"),
        event_count=("event_id", "count"),
        platform=("platform", "first"),
    ).reset_index()
    
    sessions["duration_seconds"] = (
        (sessions["session_end"] - sessions["session_start"]).dt.total_seconds() + 
        np.random.exponential(120, len(sessions))  # add reading time
    ).astype(int)
    sessions["session_id_unique"] = [_id() for _ in range(len(sessions))]
    
    return sessions


# ---------------------------------------------------------------------------
# Marketing Touches
# ---------------------------------------------------------------------------

def generate_marketing(users_df: pd.DataFrame, cfg: GeneratorConfig) -> pd.DataFrame:
    console.print("[bold blue]Generating marketing touches...[/]")
    touches = []
    
    campaigns = [
        "spring_promo_2023", "summer_launch_2023", "black_friday_2023",
        "new_year_2024", "product_hunt_launch", "webinar_series_q2",
        "partner_referral_prog", "content_seo_push", "retargeting_q3",
        "enterprise_outreach", "free_trial_campaign", "upgrade_nudge",
    ]
    
    for _, user in users_df.iterrows():
        signup = user["signup_date"]
        # Pre-signup touches (attribution)
        n_pre = random.randint(1, 5)
        for i in range(n_pre):
            touch_date = signup - timedelta(days=random.randint(1, 60))
            touches.append({
                "touch_id": _id(),
                "user_id": user["user_id"],
                "campaign": random.choice(campaigns),
                "channel": user["acquisition_channel"] if i == n_pre - 1 else random.choice(cfg.acquisition_channels),
                "touch_timestamp": datetime.combine(touch_date, datetime.min.time().replace(hour=random.randint(8, 22))),
                "touch_type": random.choice(["impression", "click", "email_open", "webinar_attend"]),
                "is_converting_touch": i == n_pre - 1,
                "cost": round(random.uniform(0.5, 15.0), 2) if "paid" in user["acquisition_channel"] else 0,
            })
    
    return pd.DataFrame(touches)


# ---------------------------------------------------------------------------
# Seeds (reference data)
# ---------------------------------------------------------------------------

def generate_seeds(cfg: GeneratorConfig) -> dict[str, pd.DataFrame]:
    plans_df = pd.DataFrame([
        {"plan_id": f"plan_{k}", "plan_name": k, "monthly_price": v["price"],
         "annual_price": round(v["price"] * 12 * 0.85, 2), "tier_order": i + 1,
         "features": ", ".join(v["features"])}
        for i, (k, v) in enumerate(cfg.plans.items())
    ])
    
    channels_df = pd.DataFrame([
        {"channel_name": c, "channel_type": "paid" if "paid" in c else "organic",
         "default_cac": round(random.uniform(5, 80), 2)}
        for c in cfg.acquisition_channels
    ])
    
    return {"plan_details": plans_df, "channel_details": channels_df}


# ---------------------------------------------------------------------------
# Main Generator
# ---------------------------------------------------------------------------

def run(cfg: GeneratorConfig | None = None) -> None:
    cfg = cfg or GeneratorConfig()
    output = Path(cfg.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    seed_dir = Path("dbt_metricflow/seeds")
    seed_dir.mkdir(parents=True, exist_ok=True)
    
    console.print("[bold green]━━━ MetricFlow Data Generator ━━━[/]")
    console.print(f"  Users: {cfg.num_users:,}")
    console.print(f"  Period: {cfg.history_start} → {cfg.history_end}")
    console.print()
    
    # Generate each dataset
    users = generate_users(cfg)
    subs = generate_subscriptions(users, cfg)
    payments = generate_payments(subs, cfg)
    events = generate_events(users, subs, cfg)
    sessions = generate_sessions(events)
    marketing = generate_marketing(users, cfg)
    seeds = generate_seeds(cfg)
    
    # Write Parquet files
    datasets = {
        "users": users,
        "subscriptions": subs,
        "payments": payments,
        "events": events,
        "sessions": sessions,
        "marketing_touches": marketing,
    }
    
    console.print("\n[bold blue]Writing Parquet files...[/]")
    for name, df in datasets.items():
        path = output / f"{name}.parquet"
        df.to_parquet(path, compression=cfg.parquet_compression, index=False)
        console.print(f"  ✓ {name}: {len(df):>12,} rows → {path}")
    
    # Write seed CSVs
    for name, df in seeds.items():
        path = seed_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        console.print(f"  ✓ seed/{name}: {len(df)} rows → {path}")
    
    total = sum(len(df) for df in datasets.values())
    console.print(f"\n[bold green]✅ Generated {total:,} total rows across {len(datasets)} datasets[/]")


if __name__ == "__main__":
    run()
