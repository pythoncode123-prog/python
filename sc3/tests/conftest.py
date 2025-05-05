"""
Test configuration and fixtures for the Change Scheduler service.
"""
import pytest
import os
import tempfile
import datetime
from unittest.mock import patch, MagicMock

# Create a mock datetime class before imports
class MockDatetime(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return datetime.datetime(2025, 5, 6, 0, 0, 0)

@pytest.fixture
def mock_datetime():
    """Mock datetime.utcnow to return a fixed date."""
    with patch('datetime.datetime', MockDatetime):
        yield MockDatetime

@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        old_data_dir = os.environ.get('SCHEDULER_DATA_DIR')
        os.environ['SCHEDULER_DATA_DIR'] = temp_dir
        yield temp_dir
        if old_data_dir:
            os.environ['SCHEDULER_DATA_DIR'] = old_data_dir
        else:
            del os.environ['SCHEDULER_DATA_DIR']

@pytest.fixture
def mock_requests():
    """Mock requests.get for testing."""
    with patch('requests.get') as mock_get:
        # Set up default mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "approved"}
        mock_get.return_value = mock_response
        yield mock_get

@pytest.fixture
def test_client(temp_data_dir):
    """Create a test client for the FastAPI app."""
    from fastapi.testclient import TestClient
    from api.app import create_app
    
    app = create_app()
    return TestClient(app)

@pytest.fixture
def task_manager():
    """Create a task manager instance for testing."""
    from scheduler.task_manager import TaskManager
    
    manager = TaskManager()
    manager.start()
    yield manager
    manager.stop()