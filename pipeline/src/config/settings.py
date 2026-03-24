"""Configuration loading for the NAAP Analytics pipeline.

Layer 2 — imports from types only.
All settings are loaded from environment variables at startup.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline settings loaded from environment variables.

    See .env.example for documentation of each variable.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Environment
    env: str = Field(default="development")
    log_level: str = Field(default="info")

    # Kafka
    kafka_brokers: str = Field(..., description="Comma-separated list of Kafka broker addresses")
    kafka_group_id: str = Field(default="naap-analytics-pipeline")
    kafka_auto_offset_reset: str = Field(default="earliest")
    kafka_session_timeout_ms: int = Field(default=30_000)

    # Topics (consumed)
    topic_raw_events: str = Field(default="naap.events.raw")

    # Topics (produced)
    topic_aggregated: str = Field(default="naap.analytics.aggregated")
    topic_alerts: str = Field(default="naap.alerts")

    # Telemetry
    otlp_endpoint: str = Field(default="")

    @property
    def brokers(self) -> list[str]:
        return [b.strip() for b in self.kafka_brokers.split(",")]

    @property
    def is_development(self) -> bool:
        return self.env == "development"
