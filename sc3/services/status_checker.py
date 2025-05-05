"""
Status checking for scheduled changes.
"""
import threading
import time
import logging
import requests
from core.config import STATUS_CHECK_ENDPOINT, STATUS_CHECK_INTERVAL, STATUS_CHECK_TIMEOUT

logger = logging.getLogger("scheduler")

def check_change_status(change_number):
    """Check the status of a change."""
    try:
        response = requests.get(
            f"{STATUS_CHECK_ENDPOINT}?change_number={change_number}", 
            timeout=STATUS_CHECK_TIMEOUT
        )
        if response.status_code == 200:
            data = response.json()
            status = data.get('status')
            logger.info(f"Change {change_number} status: {status}")
            return status
        else:
            logger.warning(
                f"Failed to get status for change {change_number}, "
                f"status code: {response.status_code}"
            )
    except Exception as e:
        logger.error(f"Error checking status for change {change_number}: {str(e)}")
    
    return None

class StatusChecker:
    """Periodically checks the status of scheduled changes."""
    
    def __init__(self, task_store):
        """Initialize the status checker."""
        self.task_store = task_store
        self.is_running = False
        self.thread = None
    
    def start(self):
        """Start the status checker."""
        if self.is_running:
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._checker_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Status checker started (interval: {STATUS_CHECK_INTERVAL}s)")
    
    def stop(self):
        """Stop the status checker."""
        if not self.is_running:
            return
        
        self.is_running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
        logger.info("Status checker stopped")
    
    def _checker_loop(self):
        """Main status checking loop."""
        while self.is_running:
            try:
                # Sleep at the start
                time.sleep(STATUS_CHECK_INTERVAL)
                
                if not self.is_running:
                    break
                
                # Get all change numbers
                change_numbers = self.task_store.get_all_change_numbers()
                
                if not change_numbers:
                    continue
                
                logger.debug(f"Checking status for {len(change_numbers)} changes")
                
                # Check status for each change
                for change_number in change_numbers:
                    status = check_change_status(change_number)
                    
                    # If canceled, remove from task store
                    if status == "canceled":
                        logger.info(f"Removing change {change_number} as it was canceled")
                        self.task_store.remove(change_number)
            except Exception as e:
                logger.error(f"Error in status checker loop: {str(e)}")
                time.sleep(10)  # Wait a bit before retrying