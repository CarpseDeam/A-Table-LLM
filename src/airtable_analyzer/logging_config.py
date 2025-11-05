"""Logging configuration helpers."""

import logging
import sys
from typing import Optional


def configure_logging(level: int = logging.INFO) -> None:
    """Configure application-wide logging.

    Args:
        level: Minimum logging level.
    """
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [handler]


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a configured logger instance."""
    return logging.getLogger(name)
