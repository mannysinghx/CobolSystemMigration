"""Global application settings loaded from environment / .env file."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    database_url: str = "postgresql+psycopg://cobolshift:secret@localhost:5432/cobolshift"
    database_pool_size: int = 10

    # Redis / Celery
    redis_url: str = "redis://localhost:6379/0"

    # Security
    secret_key: str = "change-me"
    fernet_key: str = ""
    access_token_expire_minutes: int = 480

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = False
    log_level: str = "INFO"

    # CDC
    kafka_bootstrap_servers: str = "localhost:9092"
    debezium_connect_url: str = "http://localhost:8083"

    # Upload storage
    upload_dir: str = "./uploads"


@lru_cache
def get_settings() -> Settings:
    return Settings()
