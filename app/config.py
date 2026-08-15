import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application configuration loaded strictly from environment variables.
    No hardcoded secrets or infrastructure targets.
    """
    pseudogram_api_key: str = ""
    pseudogram_base_url: str = "https://pseudogram-api.onrender.com"
    db_path: str = "data/app.db"
    enable_signature_verification: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()
