"""
API data models.
"""
from pydantic import BaseModel, validator
from typing import Dict, Any, List, Optional
from utils.datetime_utils import is_future_datetime

class ScheduleChangeRequest(BaseModel):
    """Model for schedule change requests."""
    change_number: str
    implementation_time: str
    
    @validator('implementation_time')
    def validate_implementation_time(cls, v):
        """Validate the implementation time is in the future."""
        if not is_future_datetime(v):
            raise ValueError("Implementation time must be in the future")
        return v

class ScheduleResponse(BaseModel):
    """Model for schedule change responses."""
    change_number: str
    status: str
    implementation_time: str

class HealthResponse(BaseModel):
    """Model for health check responses."""
    status: str
    version: str
    current_time: str
    tasks_count: int
    scheduler_running: bool
    next_task: Optional[Dict[str, str]] = None
    status_check_interval: str

class StatusCheckResponse(BaseModel):
    """Model for status check responses."""
    change_number: str
    current_status: Optional[str] = None