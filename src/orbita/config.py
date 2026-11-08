"""Configuration supplied by the caller, never by another repository."""

from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigurationError(ValueError):
    """Safe, explicit configuration diagnostic."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", hide_input_in_errors=True)

    app_env: Literal["development", "staging", "production"] = "development"
    llm_model: str = Field(default="gpt-6-luna", min_length=1)
    openai_api_key: SecretStr = SecretStr("")
    langfuse_base_url: Literal["http://localhost:3000", "http://127.0.0.1:3000"] = (
        "http://localhost:3000"
    )
    langfuse_project_name: str = "orbita"
    langfuse_public_key: SecretStr = SecretStr("")
    langfuse_secret_key: SecretStr = SecretStr("")

    def require_services(self, *, live: bool) -> None:
        required = {
            "LANGFUSE_PUBLIC_KEY": self.langfuse_public_key,
            "LANGFUSE_SECRET_KEY": self.langfuse_secret_key,
        }
        if live:
            required["OPENAI_API_KEY"] = self.openai_api_key
        for name, value in required.items():
            if not value.get_secret_value().strip():
                raise ConfigurationError(f"Set {name} in the local environment file")
