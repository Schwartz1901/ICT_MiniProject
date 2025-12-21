"""
Centralized logging configuration for the pipeline.
"""

import logging


def setup_logging(level=logging.INFO):
    """Configure logging for the entire application."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
