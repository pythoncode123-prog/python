"""
Task scheduling and execution for the Change Scheduler service.
"""
import threading
import time
import queue
import datetime
import logging
import requests
from core.config import TARGET_ENDPOINT, REQUEST_TIMEOUT
from core.storage import save_tasks
from utils.datetime_utils import parse_datetime
from services.status_checker import check_change_status

logger = logging.getLogger("scheduler")

class TaskScheduler:
    """Scheduler for executing tasks at their scheduled times."""
    
    def __init__(self, task_store):
        """Initialize the task scheduler."""
        self.task_store = task_store
        self.task_queue = queue.PriorityQueue()
        self.is_running = False
        self.thread = None
    
    def start(self):
        """Start the scheduler."""
        if self.is_running:
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._scheduler_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info("Task scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        if not self.is_running:
            return
        
        self.is_running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
        logger.info("Task scheduler stopped")
    
    def add_task(self, change_number, implementation_time):
        """Add a task to the scheduler queue."""
        try:
            dt = parse_datetime(implementation_time)
            self.task_queue.put((dt.timestamp(), change_number))
            logger.debug(f"Added task {change_number} to scheduler queue")
        except Exception as e:
            logger.error(f"Error adding task to scheduler queue: {str(e)}")
    
    def execute_task(self, task):
        """Execute a scheduled task."""
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
        except Exception as e:
            logger.error(f"Error executing task: {str(e)}")
    
    def _scheduler_loop(self):
        """Main scheduling loop."""
        while self.is_running:
            try:
                # Check if there are any tasks in the queue
                if not self.task_queue.empty():
                    # Peek at the next task
                    next_time, change_number = self.task_queue.queue[0]
                    now = datetime.datetime.utcnow().timestamp()
                    
                    # If it's time to execute
                    if next_time <= now:
                        # Remove from queue
                        self.task_queue.get()
                        
                        # Get task details and execute
                        task = self.task_store.get(change_number)
                        if task:
                            # Check status one last time before executing
                            status = check_change_status(change_number)
                            
                            # Only execute if the change wasn't canceled
                            if status != "canceled":
                                # Execute the task
                                threading.Thread(target=self.execute_task, args=(task,)).start()
                                
                                # Remove from task store
                                self.task_store.remove(change_number)
                            else:
                                logger.info(f"Skipping execution of change {change_number} as it was canceled")
                    else:
                        # Wait until the next task is due, but check every second
                        time.sleep(min(next_time - now, 1.0))
                else:
                    # No tasks, check again in a second
                    time.sleep(1.0)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(1.0)