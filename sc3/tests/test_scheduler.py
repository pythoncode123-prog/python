"""
Tests for the scheduler components.
"""
import pytest
import time
import threading
from unittest.mock import patch, MagicMock

from scheduler.task_manager import TaskManager
from scheduler.status_checker import check_change_status
from scheduler.execution import execute_task

def test_task_manager_add_task(task_manager):
    """Test adding a task to the task manager."""
    # Create a test task
    task = {
        'change_number': 'CHG123456',
        'implementation_time': '2025-05-06 10:00:00',
        'created_at': '2025-05-06 00:00:00'
    }
    
    # Add the task
    task_manager.add_task(task)
    
    # Check that the task was added
    assert 'CHG123456' in task_manager.scheduled_tasks
    assert task_manager.scheduled_tasks['CHG123456'] == task

def test_task_manager_remove_task(task_manager):
    """Test removing a task from the task manager."""
    # Create and add a test task
    task = {
        'change_number': 'CHG123456',
        'implementation_time': '2025-05-06 10:00:00',
        'created_at': '2025-05-06 00:00:00'
    }
    task_manager.add_task(task)
    
    # Remove the task
    result = task_manager.remove_task('CHG123456')
    
    # Check that the task was removed
    assert result == True
    assert 'CHG123456' not in task_manager.scheduled_tasks

def test_task_manager_remove_nonexistent_task(task_manager):
    """Test removing a nonexistent task from the task manager."""
    # Try to remove a nonexistent task
    result = task_manager.remove_task('NONEXISTENT')
    
    # Check that the removal failed
    assert result == False

def test_task_manager_get_task(task_manager):
    """Test getting a task from the task manager."""
    # Create and add a test task
    task = {
        'change_number': 'CHG123456',
        'implementation_time': '2025-05-06 10:00:00',
        'created_at': '2025-05-06 00:00:00'
    }
    task_manager.add_task(task)
    
    # Get the task
    result = task_manager.get_task('CHG123456')
    
    # Check that the correct task was returned
    assert result == task

def test_task_manager_get_all_tasks(task_manager):
    """Test getting all tasks from the task manager."""
    # Create and add some test tasks
    task1 = {
        'change_number': 'CHG123456',
        'implementation_time': '2025-05-06 10:00:00',
        'created_at': '2025-05-06 00:00:00'
    }
    task2 = {
        'change_number': 'CHG789012',
        'implementation_time': '2025-05-07 10:00:00',
        'created_at': '2025-05-06 00:00:00'
    }
    task_manager.add_task(task1)
    task_manager.add_task(task2)
    
    # Get all tasks
    result = task_manager.get_all_tasks()
    
    # Check that all tasks were returned
    assert len(result) == 2
    change_numbers = [task['change_number'] for task in result]
    assert 'CHG123456' in change_numbers
    assert 'CHG789012' in change_numbers

@patch('scheduler.status_checker.requests.get')
def test_check_change_status(mock_get):
    """Test checking the status of a change."""
    # Set up the mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "approved"}
    mock_get.return_value = mock_response
    
    # Check the status of a change
    status = check_change_status('CHG123456')
    
    # Verify the result
    assert status == "approved"
    mock_get.assert_called_once()

@patch('scheduler.execution.requests.get')
def test_execute_task(mock_get):
    """Test executing a task."""
    # Set up the mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    
    # Create a test task
    task = {
        'change_number': 'CHG123456',
        'implementation_time': '2025-05-06 10:00:00',
        'created_at': '2025-05-06 00:00:00'
    }
    
    # Execute the task
    status_code = execute_task(task)
    
    # Verify the result
    assert status_code == 200
    mock_get.assert_called_once()