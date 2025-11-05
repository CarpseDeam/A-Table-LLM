"""Application configuration and environment management."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables.

    Attributes:
        airtable_access_token: Airtable personal access token with metadata scope.
        airtable_base_id: Identifier of the Airtable base to analyze.
        gemini_api_key: Google Gemini API key.
        gemini_model: Gemini model name to use for analysis.
        request_timeout_seconds: HTTP timeout for outbound API requests.
        max_retry_attempts: Maximum number of retry attempts for transient failures.
        initial_backoff_seconds: Initial backoff used when retrying failed requests.
    """

    airtable_access_token: SecretStr = Field(..., alias="AIRTABLE_ACCESS_TOKEN")
    airtable_base_id: str = Field(..., alias="AIRTABLE_BASE_ID")
    gemini_api_key: SecretStr = Field(..., alias="GEMINI_API_KEY")
    gemini_model: Literal["gemini-2.5-pro", "gemini-2.5-flash"] = Field(
        default="gemini-2.5-pro", alias="GEMINI_MODEL"
    )
    request_timeout_seconds: int = Field(default=30, alias="REQUEST_TIMEOUT_SECONDS")
    max_retry_attempts: int = Field(default=5, alias="MAX_RETRY_ATTEMPTS")
    initial_backoff_seconds: float = Field(default=0.5, alias="INITIAL_BACKOFF_SECONDS")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    def get_airtable_token(self) -> str:
        """Return the Airtable access token as a plain string."""
        return self.airtable_access_token.get_secret_value()

    def get_gemini_api_key(self) -> str:
        """Return the Gemini API key as a plain string."""
        return self.gemini_api_key.get_secret_value()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings instance."""
    return Settings()
