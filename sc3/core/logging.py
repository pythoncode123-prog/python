"""
Logging configuration for the Change Scheduler service.
"""
import logging
import os

def setup_logger():
    """Configure and return logger for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("scheduler.log")
        ]
    )
    return logging.getLogger("scheduler")

# Create logger instance
logger = setup_logger()