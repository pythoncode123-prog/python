"""
Task execution functionality for the Change Scheduler service.
"""
import threading
import requests
from core.config import TARGET_ENDPOINT, REQUEST_TIMEOUT
from core.logging import logger

def execute_task(task):
    """
    Execute a scheduled task by making an HTTP request to the target endpoint.
    
    Args:
        task: A dictionary containing task details
        
    Returns:
        Status code of the HTTP response or None if an error occurred
    """
    try:
        change_number = task['change_number']
        implementation_time = task['implementation_time']
        logger.info(f"Executing change {change_number} scheduled for {implementation_time}")
        
        # Make HTTP request to the target endpoint
        try:
            response = requests.get(TARGET_ENDPOINT, timeout=REQUEST_TIMEOUT)
            status_code = response.status_code
            logger.info(f"Task {change_number} execution completed with status code {status_code}")
            return status_code
        except Exception as e:
            logger.error(f"HTTP request failed for task {change_number}: {str(e)}")
            return None
        
    except Exception as e:
        logger.error(f"Error executing task: {str(e)}")
        return None

def execute_task_async(task):
    """
    Execute a task asynchronously in a separate thread.
    
    Args:
        task: A dictionary containing task details
    """
    thread = threading.Thread(target=execute_task, args=(task,))
    thread.daemon = True
    thread.start()
    return thread