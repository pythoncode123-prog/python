"""
Entry point for the Change Scheduler service.
"""
import uvicorn
from api.app import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")