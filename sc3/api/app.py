"""
FastAPI application.
"""
from fastapi import FastAPI
import logging

from api.routes import router
from core.storage import load_tasks
from services.scheduler import TaskScheduler
from services.status_checker import StatusChecker

logger = logging.getLogger("scheduler")

class TaskStore:
    """In-memory store for tasks with file persistence."""
    
    def __init__(self, initial_tasks=None):
        """Initialize the task store."""
        self.tasks = initial_tasks or {}
        from core.storage import save_tasks
        self.save_tasks = save_tasks
    
    def add(self, task):
        """Add a task to the store."""
        change_number = task['change_number']
        self.tasks[change_number] = task
        self.save_tasks(self.tasks)
        return task
    
    def get(self, change_number):
        """Get a task from the store."""
        return self.tasks.get(change_number)
    
    def remove(self, change_number):
        """Remove a task from the store."""
        if change_number in self.tasks:
            del self.tasks[change_number]
            self.save_tasks(self.tasks)
            return True
        return False
    
    def get_all(self):
        """Get all tasks."""
        return list(self.tasks.values())
    
    def get_all_change_numbers(self):
        """Get all change numbers."""
        return list(self.tasks.keys())

def create_app():
    """Create and configure the FastAPI application."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("scheduler.log")
        ]
    )
    
    # Create FastAPI app
    app = FastAPI(
        title="Change Scheduler API",
        description="API for scheduling changes at specific implementation times with status monitoring",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc"
    )
    
    # Include router
    app.include_router(router)
    
    # Load initial tasks
    initial_tasks = load_tasks()
    
    # Create task store
    task_store = TaskStore(initial_tasks)
    
    # Create scheduler
    scheduler = TaskScheduler(task_store)
    
    # Create status checker
    status_checker = StatusChecker(task_store)
    
    # Set dependencies in routes
    import api.routes as routes
    routes.task_store = task_store
    routes.scheduler = scheduler
    routes.status_checker = status_checker
    
    # Startup event
    @app.on_event("startup")
    async def startup_event():
        """Initialize on startup."""
        # Start scheduler
        scheduler.start()
        
        # Start status checker
        status_checker.start()
        
        logger.info("Change Scheduler service started")
    
    # Shutdown event
    @app.on_event("shutdown")
    async def shutdown_event():
        """Clean up on shutdown."""
        # Stop scheduler
        scheduler.stop()
        
        # Stop status checker
        status_checker.stop()
        
        logger.info("Change Scheduler service stopped")
    
    return app