"""
Application configuration using pydantic-settings.
"""

from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Database
    database_url: str = "postgresql://river_router:password@localhost:5432/river_router"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Data paths
    graph_path: str = "/app/data/graph/national.pkl"
    nhdplus_path: str = "/app/data/nhdplus/"

    # NWM Settings
    nwm_bucket: str = "noaa-nwm-pds"
    nwm_region: str = "us-east-1"
    nwm_cache_ttl: int = 3600  # seconds

    # API Settings
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4
    debug: bool = False

    # CORS
    cors_origins_str: str = "http://localhost:3000"

    # Logging
    log_level: str = "INFO"

    @property
    def cors_origins(self) -> List[str]:
        """Parse CORS origins from comma-separated string."""
        return [origin.strip() for origin in self.cors_origins_str.split(",")]


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Global settings instance
settings = get_settings()
