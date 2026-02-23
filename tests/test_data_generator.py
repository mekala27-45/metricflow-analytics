"""Tests for data generator."""
import pandas as pd
from data_generator.config import GeneratorConfig
from data_generator.generate import generate_users, generate_subscriptions


def test_generate_users_count():
    cfg = GeneratorConfig(num_users=100)
    users = generate_users(cfg)
    assert len(users) == 100


def test_user_columns():
    cfg = GeneratorConfig(num_users=50)
    users = generate_users(cfg)
    expected = {"user_id", "email", "name", "signup_date", "country",
                "acquisition_channel", "initial_plan"}
    assert expected.issubset(set(users.columns))


def test_no_null_user_ids():
    cfg = GeneratorConfig(num_users=50)
    users = generate_users(cfg)
    assert users["user_id"].notna().all()


def test_subscription_references_users():
    cfg = GeneratorConfig(num_users=50)
    users = generate_users(cfg)
    subs = generate_subscriptions(users, cfg)
    assert set(subs["user_id"]).issubset(set(users["user_id"]))


def test_plan_names_valid():
    cfg = GeneratorConfig(num_users=50)
    users = generate_users(cfg)
    subs = generate_subscriptions(users, cfg)
    valid_plans = set(cfg.plans.keys())
    assert set(subs["plan"]).issubset(valid_plans)
