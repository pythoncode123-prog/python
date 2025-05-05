"""
Task management and scheduling for the Change Scheduler service.
"""
import threading
import time
import queue
import datetime
from core.logging import logger
from core.storage import save_tasks
from utils.datetime_utils import parse_datetime
from scheduler.status_checker import check_change_status
from scheduler.execution import execute_task_async

class TaskManager:
    """
    Manager for scheduled tasks, handles scheduling, execution, and status checking.
    """
    
    def __init__(self):
        """Initialize the task manager."""
        self.scheduled_tasks = {}
        self.task_queue = queue.PriorityQueue()
        self.is_running = False
        self.scheduler_thread = None
        self.status_checker_thread = None
        self.file_lock = threading.Lock()
        
    def add_task(self, task):
        """
        Add a task to the schedule.
        
        Args:
            task: A dictionary containing task details
            
        Returns:
            The added task
        """
        change_number = task['change_number']
        implementation_time = task['implementation_time']
        
        # Parse the implementation time to get a timestamp for the queue
        dt = parse_datetime(implementation_time)
        timestamp = dt.timestamp()
        
        # Store task in memory
        with self.file_lock:
            if change_number in self.scheduled_tasks:
                old_time = self.scheduled_tasks[change_number]['implementation_time']
                logger.info(f"Rescheduling change {change_number} from {old_time} to {implementation_time}")
                
            self.scheduled_tasks[change_number] = task
            
            # Add to priority queue
            self.task_queue.put((timestamp, change_number))
        
        # Save to file
        save_tasks(self.scheduled_tasks)
        
        # Check initial status
        threading.Thread(
            target=lambda: check_change_status(change_number),
            daemon=True
        ).start()
        
        logger.info(f"Scheduled change {change_number} for {implementation_time}")
        
        return task
        
    def remove_task(self, change_number):
        """
        Remove a task from the schedule.
        
        Args:
            change_number: The unique identifier of the task to remove
            
        Returns:
            True if the task was removed, False otherwise
        """
        with self.file_lock:
            if change_number not in self.scheduled_tasks:
                return False
                
            del self.scheduled_tasks[change_number]
            
        # Save to file
        save_tasks(self.scheduled_tasks)
        
        logger.info(f"Removed change {change_number} from schedule")
        return True
        
    def get_task(self, change_number):
        """
        Get a task from the schedule.
        
        Args:
            change_number: The unique identifier of the task to get
            
        Returns:
            The task if found, None otherwise
        """
        return self.scheduled_tasks.get(change_number)
        
    def get_all_tasks(self):
        """
        Get all scheduled tasks.
        
        Returns:
            A list of all scheduled tasks
        """
        return list(self.scheduled_tasks.values())
        
    def start(self, initial_tasks=None):
        """
        Start the task manager.
        
        Args:
            initial_tasks: Optional dictionary of tasks to initialize with
        """
        if self.is_running:
            return
            
        # Initialize tasks if provided
        if initial_tasks:
            self.scheduled_tasks = initial_tasks
            
            # Add tasks to priority queue
            for change_number, task in initial_tasks.items():
                try:
                    dt = parse_datetime(task['implementation_time'])
                    self.task_queue.put((dt.timestamp(), change_number))
                except Exception as e:
                    logger.error(f"Error adding task {change_number} to queue: {str(e)}")
        
        # Start threads
        self.is_running = True
        
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        
        self.status_checker_thread = threading.Thread(target=self._status_checker_loop)
        self.status_checker_thread.daemon = True
        self.status_checker_thread.start()
        
        logger.info("Task manager started")
        
    def stop(self):
        """Stop the task manager."""
        if not self.is_running:
            return
            
        self.is_running = False
        
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5.0)
            
        if self.status_checker_thread and self.status_checker_thread.is_alive():
            self.status_checker_thread.join(timeout=5.0)
            
        logger.info("Task manager stopped")
        
    def _scheduler_loop(self):
        """
        Main loop that checks for and executes scheduled tasks.
        
        This runs in a separate thread.
        """
        logger.info("Task execution scheduler started")
        
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
                        with self.file_lock:
                            if change_number in self.scheduled_tasks:
                                # Check status one last time before executing
                                status = check_change_status(change_number)
                                
                                # Only execute if the change wasn't canceled
                                if status != "canceled":
                                    task = self.scheduled_tasks[change_number]
                                    
                                    # Remove from scheduled tasks
                                    del self.scheduled_tasks[change_number]
                                    
                                    # Update the file
                                    save_tasks(self.scheduled_tasks)
                                    
                                    # Execute in a separate thread
                                    execute_task_async(task)
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
                
    def _status_checker_loop(self):
        """
        Loop that periodically checks the status of all scheduled changes.
        
        This runs in a separate thread.
        """
        from core.config import STATUS_CHECK_INTERVAL
        
        logger.info(f"Status checker started (checking every {STATUS_CHECK_INTERVAL} seconds)")
        
        while self.is_running:
            try:
                # Sleep at the start to allow server to fully initialize
                time.sleep(STATUS_CHECK_INTERVAL)
                
                if not self.is_running:  # Check if we should stop
                    break
                    
                # Check status for each scheduled change
                with self.file_lock:
                    change_numbers = list(self.scheduled_tasks.keys())
                
                logger.info(f"Checking status for {len(change_numbers)} changes")
                
                changes_to_remove = []
                for change_number in change_numbers:
                    try:
                        status = check_change_status(change_number)
                        
                        # If the change was canceled, mark it for removal
                        if status == "canceled":
                            changes_to_remove.append(change_number)
                        
                    except Exception as e:
                        logger.error(f"Error during status check for {change_number}: {str(e)}")
                
                # Process removals outside the loop to avoid modifying the dictionary during iteration
                if changes_to_remove:
                    with self.file_lock:
                        for change_number in changes_to_remove:
                            if change_number in self.scheduled_tasks:
                                logger.info(f"Removing change {change_number} as it was canceled")
                                del self.scheduled_tasks[change_number]
                        
                        # Update the file if any changes were removed
                        if changes_to_remove:
                            save_tasks(self.scheduled_tasks)
                        
            except Exception as e:
                logger.error(f"Error in status checker loop: {str(e)}")
                time.sleep(10)  # Wait a bit before retrying if there was an error