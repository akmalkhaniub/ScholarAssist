"""
ScholarAssist Pipeline — Application Configuration

All settings are loaded from environment variables (with sensible defaults for local
Docker Compose development). In production, these are injected via ECS task definitions
or Kubernetes ConfigMaps/Secrets.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    """Deployment environment."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class S3Settings(BaseSettings):
    """S3/MinIO object storage configuration."""
    model_config = SettingsConfigDict(env_prefix="S3_")

    endpoint_url: str = "http://localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    region: str = "us-east-1"
    use_ssl: bool = False

    # Bucket names (Medallion architecture)
    raw_bucket: str = "scholarassist-raw"
    normalized_bucket: str = "scholarassist-normalized"
    golden_bucket: str = "scholarassist-golden"
    manifests_bucket: str = "scholarassist-manifests"
    dead_letter_bucket: str = "scholarassist-dead-letter"
    logs_bucket: str = "scholarassist-logs"


class OpenSearchSettings(BaseSettings):
    """OpenSearch cluster configuration."""
    model_config = SettingsConfigDict(env_prefix="OPENSEARCH_")

    url: str = "http://localhost:9200"
    username: Optional[str] = None
    password: Optional[str] = None
    use_ssl: bool = False
    verify_certs: bool = False
    timeout: int = 60

    # Index configuration
    index_alias: str = "scholar_works"
    index_prefix: str = "scholar_works_v"
    number_of_shards: int = 3
    number_of_replicas: int = 1

    @field_validator("number_of_shards", "number_of_replicas")
    @classmethod
    def validate_positive(cls, v: int) -> int:
        if v < 0:
            raise ValueError("Must be >= 0")
        return v


class SparkSettings(BaseSettings):
    """Apache Spark configuration."""
    model_config = SettingsConfigDict(env_prefix="SPARK_")

    master_url: str = "spark://localhost:7077"
    app_name: str = "ScholarAssist-Pipeline"
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    executor_cores: int = 2
    shuffle_partitions: int = 200

    # Delta Lake settings
    delta_log_retention_hours: int = 168  # 7 days


class RedisSettings(BaseSettings):
    """Redis configuration for rate limiting and caching."""
    model_config = SettingsConfigDict(env_prefix="REDIS_")

    url: str = "redis://localhost:6379/0"
    max_connections: int = 20


class APISettings(BaseSettings):
    """FastAPI service configuration."""
    model_config = SettingsConfigDict(env_prefix="API_")

    secret_key: str = "dev-secret-key-change-in-production"
    debug: bool = True
    title: str = "ScholarAssist Academic Dataset API"
    description: str = "REST API for accessing 300M+ academic records"
    version: str = "1.0.0"

    # Rate limiting
    rate_limit_requests: int = 100
    rate_limit_window_seconds: int = 60

    # Pagination
    default_page_size: int = 20
    max_page_size: int = 100

    # CORS
    cors_origins: list[str] = ["*"]


class ProviderSettings(BaseSettings):
    """Data provider API keys and configuration."""
    model_config = SettingsConfigDict(env_prefix="PROVIDER_")

    # OpenAlex
    openalex_email: Optional[str] = None  # Polite pool access
    openalex_api_key: Optional[str] = None

    # Crossref
    crossref_email: Optional[str] = None  # Polite pool access
    crossref_plus_token: Optional[str] = None

    # Semantic Scholar
    semantic_scholar_api_key: Optional[str] = None

    # CORE
    core_api_key: Optional[str] = None

    # Provider priority for Golden Record conflict resolution (highest first)
    provider_priority: list[str] = Field(
        default=[
            "crossref",
            "openalex",
            "semantic_scholar",
            "core",
            "unpaywall",
            "dblp",
            "opencitations",
        ]
    )


class Settings(BaseSettings):
    """Root application settings — aggregates all sub-settings."""
    model_config = SettingsConfigDict(
        env_prefix="SCHOLARASSIST_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: Environment = Environment.DEVELOPMENT

    s3: S3Settings = Field(default_factory=S3Settings)
    opensearch: OpenSearchSettings = Field(default_factory=OpenSearchSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    api: APISettings = Field(default_factory=APISettings)
    providers: ProviderSettings = Field(default_factory=ProviderSettings)

    @property
    def is_production(self) -> bool:
        return self.env == Environment.PRODUCTION


# Singleton instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the application settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
