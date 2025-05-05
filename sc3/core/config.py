"""
Configuration settings for the Change Scheduler service.
"""
import os

# Basic settings
DATA_DIR = os.environ.get("SCHEDULER_DATA_DIR", "data")
SCHEDULED_TASKS_FILE = os.path.join(DATA_DIR, "scheduled_tasks.json")

# Endpoints
TARGET_ENDPOINT = os.environ.get("SCHEDULER_TARGET_ENDPOINT", "https://yahoo.com")
STATUS_CHECK_ENDPOINT = os.environ.get("SCHEDULER_STATUS_CHECK_ENDPOINT", "http://localhost:3001/change_status")

# Time settings
STATUS_CHECK_INTERVAL = int(os.environ.get("SCHEDULER_STATUS_CHECK_INTERVAL", "60"))
REQUEST_TIMEOUT = int(os.environ.get("SCHEDULER_REQUEST_TIMEOUT", "30"))
STATUS_CHECK_TIMEOUT = int(os.environ.get("SCHEDULER_STATUS_CHECK_TIMEOUT", "10"))

# Logging
LOG_LEVEL = os.environ.get("SCHEDULER_LOG_LEVEL", "INFO")
LOG_FILE = os.environ.get("SCHEDULER_LOG_FILE", "scheduler.log")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)