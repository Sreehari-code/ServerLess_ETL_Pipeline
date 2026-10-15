"""
Centralized Logging Utility.

Configures structured logging with safe redaction of sensitive credentials.
"""

import logging
import re
import sys
from typing import Optional

# Sensitive patterns that must never appear in logs
SENSITIVE_PATTERNS = [
    re.compile(r'(?i)(access_key|secret_key|password|token|bearer|api_key)\s*[:=]\s*["\']?([^"\'\s]+)["\']?'),
    re.compile(r'(?i)(aws_secret_access_key|aws_session_token)\s*=\s*([^\s]+)'),
]


class SensitiveDataFilter(logging.Filter):
    """Logging filter that scrubs sensitive patterns from log messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            for pattern in SENSITIVE_PATTERNS:
                record.msg = pattern.sub(r'\1=********', record.msg)
        return True


def setup_logger(name: str = "serverless_etl", level: Optional[str] = None) -> logging.Logger:
    """
    Configure and retrieve a logger instance.

    Args:
        name: Name of the logger
        level: Optional log level string (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured logging.Logger
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers if already configured
    if logger.handlers:
        return logger

    log_level_str = level or "INFO"
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logger.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    handler.addFilter(SensitiveDataFilter())

    logger.addHandler(handler)
    logger.propagate = False
    return logger
