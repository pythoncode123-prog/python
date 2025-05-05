"""
API routes.
"""
from fastapi import APIRouter, HTTPException, status
from typing import List
import logging

from api.models import (
    ScheduleChangeRequest, 
    ScheduleResponse, 
    HealthResponse,
    StatusCheckResponse
)
from utils.datetime_utils import format_datetime, get_current_datetime
from core.config import STATUS_CHECK_INTERVAL
from services.status_checker import check_change_status

router = APIRouter()
logger = logging.getLogger("scheduler")

# Will be set from app.py
task_store = None
scheduler = None
status_checker = None

@router.post("/schedule", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def schedule_change(request: ScheduleChangeRequest):
    """Schedule a new change."""
    change_number = request.change_number
    implementation_time = request.implementation_time
    
    # Create task
    task = {
        'change_number': change_number,
        'implementation_time': implementation_time,
        'created_at': format_datetime(get_current_datetime())
    }
    
    # Add to task store
    task_store.add(task)
    
    # Add to scheduler
    scheduler.add_task(change_number, implementation_time)
    
    # Initial status check in background thread
    import threading
    threading.Thread(
        target=lambda: check_change_status(change_number),
        daemon=True
    ).start()
    
    logger.info(f"Scheduled change {change_number} for {implementation_time}")
    
    return ScheduleResponse(
        change_number=change_number,
        status="scheduled",
        implementation_time=implementation_time
    )

@router.get("/schedule/{change_number}", response_model=ScheduleResponse)
async def get_scheduled_change(change_number: str):
    """Get a scheduled change."""
    task = task_store.get(change_number)
    
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Change {change_number} not found in schedule"
        )
    
    return ScheduleResponse(
        change_number=change_number,
        status="scheduled",
        implementation_time=task['implementation_time']
    )

@router.delete("/schedule/{change_number}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_scheduled_change(change_number: str):
    """Cancel a scheduled change."""
    if not task_store.get(change_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Change {change_number} not found in schedule"
        )
    
    # Remove from task store
    task_store.remove(change_number)
    
    logger.info(f"Cancelled scheduled change {change_number}")
    
    return None

@router.get("/schedule", response_model=List[ScheduleResponse])
async def list_scheduled_changes():
    """List all scheduled changes."""
    tasks = task_store.get_all()
    
    return [
        ScheduleResponse(
            change_number=task['change_number'],
            status="scheduled",
            implementation_time=task['implementation_time']
        )
        for task in tasks
    ]

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Check service health."""
    tasks = task_store.get_all()
    task_count = len(tasks)
    next_task = None
    
    if task_count > 0 and not scheduler.task_queue.empty():
        try:
            next_time, change_number = scheduler.task_queue.queue[0]
            task = task_store.get(change_number)
            if task:
                next_task = {
                    "change_number": change_number,
                    "implementation_time": task['implementation_time']
                }
        except Exception as e:
            logger.error(f"Error getting next task: {str(e)}")
    
    return HealthResponse(
        status="healthy",
        version="1.0.0",
        current_time=format_datetime(get_current_datetime()),
        tasks_count=task_count,
        scheduler_running=scheduler.is_running,
        next_task=next_task,
        status_check_interval=f"{STATUS_CHECK_INTERVAL} seconds"
    )

@router.get("/check-status/{change_number}", response_model=StatusCheckResponse)
async def force_check_status(change_number: str):
    """Force a status check for a change."""
    if not task_store.get(change_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Change {change_number} not found in schedule"
        )
    
    status = check_change_status(change_number)
    
    return StatusCheckResponse(
        change_number=change_number,
        current_status=status
    )