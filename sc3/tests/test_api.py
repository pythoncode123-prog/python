"""
Tests for the API endpoints.
"""
import pytest
import datetime
from fastapi import status

def test_schedule_change(test_client, mock_requests, mock_datetime):
    """Test scheduling a new change."""
    # Create a test change request
    test_data = {
        "change_number": "CHG123456",
        "implementation_time": "2025-05-06 10:00:00"
    }
    
    # Send the request
    response = test_client.post("/schedule", json=test_data)
    
    # Check the response
    assert response.status_code == status.HTTP_201_CREATED
    assert response.json()["change_number"] == "CHG123456"
    assert response.json()["status"] == "scheduled"
    assert response.json()["implementation_time"] == "2025-05-06 10:00:00"

def test_schedule_change_past_date(test_client, mock_datetime):
    """Test scheduling a change with a past implementation time."""
    # Create a test change request with a past date
    test_data = {
        "change_number": "CHG123456",
        "implementation_time": "2024-05-06 10:00:00"  # Past date
    }
    
    # Send the request
    response = test_client.post("/schedule", json=test_data)
    
    # Check that it was rejected
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert "future" in response.json()["detail"][0]["msg"]

def test_schedule_change_invalid_date(test_client):
    """Test scheduling a change with an invalid date format."""
    # Create a test change request with an invalid date
    test_data = {
        "change_number": "CHG123456",
        "implementation_time": "invalid-date"
    }
    
    # Send the request
    response = test_client.post("/schedule", json=test_data)
    
    # Check that it was rejected
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

def test_get_scheduled_change(test_client, mock_requests, mock_datetime):
    """Test retrieving a scheduled change."""
    # Schedule a test change
    test_data = {
        "change_number": "CHG123456",
        "implementation_time": "2025-05-06 10:00:00"
    }
    test_client.post("/schedule", json=test_data)
    
    # Get the change
    response = test_client.get("/schedule/CHG123456")
    
    # Check the response
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["change_number"] == "CHG123456"
    assert response.json()["status"] == "scheduled"
    assert response.json()["implementation_time"] == "2025-05-06 10:00:00"

def test_get_nonexistent_change(test_client):
    """Test retrieving a change that doesn't exist."""
    response = test_client.get("/schedule/NONEXISTENT")
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "not found" in response.json()["detail"]

def test_delete_scheduled_change(test_client, mock_requests, mock_datetime):
    """Test canceling a scheduled change."""
    # Schedule a test change
    test_data = {
        "change_number": "CHG123456",
        "implementation_time": "2025-05-06 10:00:00"
    }
    test_client.post("/schedule", json=test_data)
    
    # Delete the change
    response = test_client.delete("/schedule/CHG123456")
    
    # Check the response
    assert response.status_code == status.HTTP_204_NO_CONTENT
    
    # Verify the change is no longer in the schedule
    get_response = test_client.get("/schedule/CHG123456")
    assert get_response.status_code == status.HTTP_404_NOT_FOUND

def test_list_scheduled_changes(test_client, mock_requests, mock_datetime):
    """Test listing all scheduled changes."""
    # Schedule some test changes
    test_data1 = {
        "change_number": "CHG123456",
        "implementation_time": "2025-05-06 10:00:00"
    }
    test_data2 = {
        "change_number": "CHG789012",
        "implementation_time": "2025-05-07 10:00:00"
    }
    test_client.post("/schedule", json=test_data1)
    test_client.post("/schedule", json=test_data2)
    
    # Get the list
    response = test_client.get("/schedule")
    
    # Check the response
    assert response.status_code == status.HTTP_200_OK
    assert len(response.json()) == 2
    
    # Check the contents of the list
    change_numbers = [item["change_number"] for item in response.json()]
    assert "CHG123456" in change_numbers
    assert "CHG789012" in change_numbers

def test_health_check(test_client, mock_requests, mock_datetime):
    """Test the health check endpoint."""
    # Schedule a test change
    test_data = {
        "change_number": "CHG123456",
        "implementation_time": "2025-05-06 10:00:00"
    }
    test_client.post("/schedule", json=test_data)
    
    # Get the health status
    response = test_client.get("/health")
    
    # Check the response
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == "healthy"
    assert response.json()["tasks_count"] == 1
    assert response.json()["scheduler_running"] == True
    assert response.json()["next_task"] is not None
    assert response.json()["next_task"]["change_number"] == "CHG123456"

def test_force_check_status(test_client, mock_requests, mock_datetime):
    """Test manually checking a change status."""
    # Schedule a test change
    test_data = {
        "change_number": "CHG123456",
        "implementation_time": "2025-05-06 10:00:00"
    }
    test_client.post("/schedule", json=test_data)
    
    # Force a status check
    response = test_client.get("/check-status/CHG123456")
    
    # Check the response
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["change_number"] == "CHG123456"
    assert response.json()["current_status"] == "approved"