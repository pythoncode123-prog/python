"""
Storage operations for the Change Scheduler service.
"""
import json
import threading
import os
import logging
from core.config import SCHEDULED_TASKS_FILE

logger = logging.getLogger("scheduler")
file_lock = threading.Lock()

def load_tasks():
    """Load scheduled tasks from the persistent storage file."""
    tasks = {}
    
    if not os.path.exists(SCHEDULED_TASKS_FILE):
        logger.info(f"Tasks file not found: {SCHEDULED_TASKS_FILE}")
        return tasks
    
    try:
        with open(SCHEDULED_TASKS_FILE, 'r') as f:
            tasks_data = json.load(f)
        
        for task in tasks_data:
            change_number = task.get('change_number')
            if change_number:
                tasks[change_number] = task
                
        logger.info(f"Loaded {len(tasks)} tasks from {SCHEDULED_TASKS_FILE}")
    except Exception as e:
        logger.error(f"Error loading tasks from file: {str(e)}")
    
    return tasks

def save_tasks(tasks):
    """Save scheduled tasks to a persistent storage file."""
    try:
        tasks_to_save = list(tasks.values())
        
        def save_in_thread():
            try:
                with file_lock, open(SCHEDULED_TASKS_FILE, 'w') as f:
                    json.dump(tasks_to_save, f, indent=2)
                logger.debug(f"Saved {len(tasks_to_save)} tasks to file")
            except Exception as e:
                logger.error(f"Error in save thread: {str(e)}")
        
        # Run in a background thread to avoid blocking API responses
        threading.Thread(target=save_in_thread, daemon=True).start()
    except Exception as e:
        logger.error(f"Error preparing save operation: {str(e)}")