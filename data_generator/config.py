"""Configuration for synthetic SaaS data generation."""

from dataclasses import dataclass, field
from datetime import date


@dataclass(frozen=True)
class GeneratorConfig:
    """Parameters controlling data generation."""

    # Scale
    num_users: int = 50_000
    history_start: date = date(2022, 1, 1)
    history_end: date = date(2025, 12, 31)

    # Subscription plans
    plans: dict = field(default_factory=lambda: {
        "free": {"price": 0, "weight": 0.40, "features": ["basic_dashboard", "5_reports"]},
        "starter": {"price": 29, "weight": 0.30, "features": ["basic_dashboard", "25_reports", "api_access"]},
        "professional": {"price": 79, "weight": 0.20, "features": ["adv_dashboard", "unlimited_reports", "api_access", "integrations", "support"]},
        "enterprise": {"price": 199, "weight": 0.10, "features": ["adv_dashboard", "unlimited_reports", "api_access", "integrations", "priority_support", "sso", "audit_log"]},
    })

    # Channels
    acquisition_channels: list = field(default_factory=lambda: [
        "organic_search", "paid_search", "social_media", "referral",
        "direct", "email_campaign", "partner", "content_marketing",
    ])

    # Churn parameters
    base_monthly_churn_rate: float = 0.045
    churn_multiplier_free: float = 2.5
    churn_multiplier_starter: float = 1.2
    churn_multiplier_pro: float = 0.6
    churn_multiplier_enterprise: float = 0.3

    # Events
    event_types: list = field(default_factory=lambda: [
        "page_view", "feature_click", "report_created", "dashboard_viewed",
        "api_call", "export_data", "invite_sent", "integration_connected",
        "search_performed", "settings_changed", "support_ticket_opened",
        "file_uploaded", "comment_added", "alert_configured",
    ])

    # Output
    output_dir: str = "data/raw"
    parquet_compression: str = "snappy"
