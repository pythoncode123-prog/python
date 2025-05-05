"""
Status checking functionality for the Change Scheduler service.
"""
import requests
from core.config import STATUS_CHECK_ENDPOINT, STATUS_CHECK_TIMEOUT
from core.logging import logger

def check_change_status(change_number):
    """
    Check the status of a change by making a request to the status endpoint.
    
    Args:
        change_number: The unique identifier of the change to check
        
    Returns:
        The status of the change, or None if the status could not be determined
    """
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
            return None
    except Exception as e:
        logger.error(f"Error checking status for change {change_number}: {str(e)}")
        return None