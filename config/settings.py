"""
Application Configuration Module.

Loads settings from environment variables and an optional .env file.
Provides safe defaults for local development without requiring AWS credentials.
"""

import os
from dataclasses import dataclass
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class Settings:
    """Immutable application settings container."""

    # AWS Configuration
    aws_region: str = os.getenv("AWS_REGION", "ap-south-1")
    s3_bucket_name: Optional[str] = os.getenv("S3_BUCKET_NAME")
    aws_profile: Optional[str] = os.getenv("AWS_PROFILE") or None

    # S3 Prefixes
    s3_input_prefix: str = os.getenv("S3_INPUT_PREFIX", "input/")
    s3_output_accepted_prefix: str = os.getenv("S3_OUTPUT_ACCEPTED_PREFIX", "output/accepted/")
    s3_output_rejected_prefix: str = os.getenv("S3_OUTPUT_REJECTED_PREFIX", "output/rejected/")
    s3_output_reports_prefix: str = os.getenv("S3_OUTPUT_REPORTS_PREFIX", "output/reports/")

    # Local Directory Paths
    local_input_dir: str = os.getenv("LOCAL_INPUT_DIR", "sample_data")
    local_output_dir: str = os.getenv("LOCAL_OUTPUT_DIR", "output")
    default_output_format: str = os.getenv("DEFAULT_OUTPUT_FORMAT", "json").lower()

    # Operational Settings
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

    @property
    def is_s3_configured(self) -> bool:
        """Returns True if minimum required S3 settings are present."""
        return bool(self.s3_bucket_name and self.s3_bucket_name.strip())


_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    """Retrieve singleton application settings instance."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance
