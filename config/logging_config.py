"""
Logging configuration for the pipeline.
"""

import logging
import sys


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure and return the pipeline logger."""
    logger = logging.getLogger("ais_pipeline")

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
