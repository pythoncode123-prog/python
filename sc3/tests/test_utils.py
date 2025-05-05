"""
Tests for utility functions.
"""
import pytest
import datetime
from utils.datetime_utils import format_datetime, parse_datetime, is_future_datetime

def test_format_datetime():
    """Test the format_datetime utility function."""
    # Test with a datetime object
    dt = datetime.datetime(2025, 5, 6, 10, 0, 0)
    assert format_datetime(dt) == "2025-05-06 10:00:00"
    
    # Test with a string
    assert format_datetime("2025-05-06 10:00:00") == "2025-05-06 10:00:00"
    
    # Test with an invalid string (should return the string as is)
    assert format_datetime("invalid") == "invalid"

def test_parse_datetime():
    """Test the parse_datetime utility function."""
    # Test with a valid datetime string
    dt = parse_datetime("2025-05-06 10:00:00")
    assert dt == datetime.datetime(2025, 5, 6, 10, 0, 0)
    
    # Test with an invalid datetime string
    with pytest.raises(ValueError):
        parse_datetime("invalid")

def test_is_future_datetime(mock_datetime):
    """Test the is_future_datetime utility function."""
    # Current time is mocked to 2025-05-06 00:00:00
    
    # Test with a future datetime
    assert is_future_datetime("2025-05-06 10:00:00") == True
    
    # Test with a past datetime
    assert is_future_datetime("2025-05-05 23:59:59") == False
    
    # Test with the current datetime
    assert is_future_datetime("2025-05-06 00:00:00") == False