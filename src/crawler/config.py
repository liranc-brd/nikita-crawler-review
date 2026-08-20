from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    rabbitmq_url: str
    fetch_service_base_url: str = "http://localhost:8000"
    artifact_root: Path = Path("output")
    crawler_batch_size: int = 10
    heartbeat_interval_seconds: int = 10
    lease_duration_seconds: int = 60
    scheduler_poll_interval_seconds: int = 30

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
