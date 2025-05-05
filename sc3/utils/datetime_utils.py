"""
Date and time utility functions.
"""
import datetime
import logging

logger = logging.getLogger("scheduler")

def format_datetime(dt_obj_or_str):
    """Format a datetime object or string to a consistent format."""
    try:
        if isinstance(dt_obj_or_str, datetime.datetime):
            return dt_obj_or_str.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(dt_obj_or_str, str):
            dt = parse_datetime(dt_obj_or_str)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(dt_obj_or_str)
    except Exception as e:
        logger.error(f"Error formatting datetime {dt_obj_or_str}: {str(e)}")
        return str(dt_obj_or_str)

def parse_datetime(dt_str):
    """Parse a datetime string to a datetime object."""
    try:
        return datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logger.error(f"Error parsing datetime: {e}")
        raise ValueError(f"Invalid datetime format: {dt_str}. Use YYYY-MM-DD HH:MM:SS")

def is_future_datetime(dt_str):
    """Check if a datetime string represents a time in the future."""
    dt = parse_datetime(dt_str)
    return dt > datetime.datetime.utcnow()

def get_current_datetime():
    """Get the current datetime in UTC."""
    return datetime.datetime.utcnow()