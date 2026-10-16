"""Structured logging configuration with secret masking and contextual logging."""

import logging
import sys
import re
from typing import Any, Dict


# Regex patterns to scrub sensitive data from log entries
SENSITIVE_PATTERNS = [
    re.compile(r"(access[-_]?token\s*[:=]\s*['\"]?)([^'\"\s]+)", re.IGNORECASE),
    re.compile(r"(api[-_]?key\s*[:=]\s*['\"]?)([^'\"\s]+)", re.IGNORECASE),
    re.compile(r"(api[-_]?secret\s*[:=]\s*['\"]?)([^'\"\s]+)", re.IGNORECASE),
    re.compile(r"(bot[-_]?token\s*[:=]\s*['\"]?)([^'\"\s]+)", re.IGNORECASE),
    re.compile(r"(client[-_]?id\s*[:=]\s*['\"]?)([^'\"\s]+)", re.IGNORECASE),
    re.compile(r"(password\s*[:=]\s*['\"]?)([^'\"\s]+)", re.IGNORECASE),
    re.compile(r"(Bearer\s+)([A-Za-z0-9_\-\.]+)", re.IGNORECASE),
    re.compile(r"(eyJ[A-Za-z0-9_\-]{15,}\.[A-Za-z0-9_\-]{15,}\.[A-Za-z0-9_\-]+)"),
]


class SensitiveDataFilter(logging.Filter):
    """Filter that strips API tokens, secrets, and credentials from log messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            msg = record.msg
            for pattern in SENSITIVE_PATTERNS:
                msg = pattern.sub(r"\1***MASKED***", msg)
            record.msg = msg
        return True


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configures root logger with structured formatting and secret scrubbing."""
    logger = logging.getLogger("options_bot")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s:%(module)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(SensitiveDataFilter())

    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


# Default logger instance
logger = setup_logging()
