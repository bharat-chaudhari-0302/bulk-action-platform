"""Application settings, loaded from the environment (12-factor)."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- Infrastructure -------------------------------------------------
    database_url: str = "postgresql+asyncpg://bulk:bulk@localhost:5432/bulk_actions"
    redis_url: str = "redis://localhost:6379/0"

    db_pool_size: int = 20
    db_max_overflow: int = 10

    # --- Batching -------------------------------------------------------
    # Entities per batch job. Large enough that one round trip does real work,
    # small enough that a retry is cheap and progress is granular.
    batch_size: int = 1000
    max_batch_size: int = 10_000

    # --- Rate limiting --------------------------------------------------
    # Per-account processing ceiling, in entities/minute. Overridable per
    # account via accounts.rate_limit_per_minute.
    default_rate_limit_per_minute: int = 10_000
    # Per-account ceiling on bulk-action submissions.
    api_rate_limit_per_minute: int = 120

    # --- Worker ---------------------------------------------------------
    worker_concurrency: int = 20
    job_max_tries: int = 3
    # Seconds a single batch job may run before arq aborts it.
    job_timeout_seconds: int = 300

    # --- Observability --------------------------------------------------
    log_level: str = "INFO"
    log_json: bool = True

    app_name: str = "bulk-action-platform"
    environment: str = Field(default="development")

    @property
    def sync_database_url(self) -> str:
        """Synchronous URL for Alembic, which runs migrations outside the event loop.

        Explicitly `+psycopg` (v3): a bare `postgresql://` URL makes SQLAlchemy
        reach for psycopg2, which this project does not install.
        """
        url = self.database_url.replace("+asyncpg", "")
        scheme, _, rest = url.partition("://")
        return f"{scheme.split('+')[0]}+psycopg://{rest}"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
