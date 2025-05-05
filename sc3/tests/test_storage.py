"""
Tests for the storage functionality.
"""
import pytest
import os
import json
import time
from core.storage import load_tasks, save_tasks
from core.config import SCHEDULED_TASKS_FILE

def test_load_tasks_file_not_exists(temp_data_dir):
    """Test loading tasks when the file doesn't exist."""
    # Ensure the file doesn't exist
    if os.path.exists(SCHEDULED_TASKS_FILE):
        os.remove(SCHEDULED_TASKS_FILE)
    
    # Load tasks
    tasks = load_tasks()
    
    # Check that no tasks were loaded
    assert len(tasks) == 0

def test_load_tasks(temp_data_dir):
    """Test loading tasks from a file."""
    # Create a test tasks file
    test_tasks = [
        {
            'change_number': 'CHG123456',
            'implementation_time': '2025-05-06 10:00:00',
            'created_at': '2025-05-06 00:00:00'
        },
        {
            'change_number': 'CHG789012',
            'implementation_time': '2025-05-07 10:00:00',
            'created_at': '2025-05-06 00:00:00'
        }
    ]
    
    os.makedirs(os.path.dirname(SCHEDULED_TASKS_FILE), exist_ok=True)
    with open(SCHEDULED_TASKS_FILE, 'w') as f:
        json.dump(test_tasks, f)
    
    # Load tasks
    tasks = load_tasks()
    
    # Check that the tasks were loaded correctly
    assert len(tasks) == 2
    assert 'CHG123456' in tasks
    assert 'CHG789012' in tasks
    assert tasks['CHG123456']['implementation_time'] == '2025-05-06 10:00:00'
    assert tasks['CHG789012']['implementation_time'] == '2025-05-07 10:00:00'

def test_save_tasks(temp_data_dir):
    """Test saving tasks to a file."""
    # Create some test tasks
    test_tasks = {
        'CHG123456': {
            'change_number': 'CHG123456',
            'implementation_time': '2025-05-06 10:00:00',
            'created_at': '2025-05-06 00:00:00'
        },
        'CHG789012': {
            'change_number': 'CHG789012',
            'implementation_time': '2025-05-07 10:00:00',
            'created_at': '2025-05-06 00:00:00'
        }
    }
    
    # Save tasks
    save_tasks(test_tasks)
    
    # Wait a bit for the background thread to complete
    time.sleep(0.5)
    
    # Check that the file was created
    assert os.path.exists(SCHEDULED_TASKS_FILE)
    
    # Load the file and check its contents
    with open(SCHEDULED_TASKS_FILE, 'r') as f:
        saved_tasks = json.load(f)
    
    assert len(saved_tasks) == 2
    assert any(task['change_number'] == 'CHG123456' for task in saved_tasks)
    assert any(task['change_number'] == 'CHG789012' for task in saved_tasks)