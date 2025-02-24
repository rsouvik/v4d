from fastapi import FastAPI, HTTPException
from celery import Celery
import random
import time

app = FastAPI()

# Setup Celery
celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

class Outcome:
    RETRIABLE_ERROR = "RETRIABLE_ERROR"
    NON_RETRIABLE_ERROR = "NON_RETRIABLE_ERROR"
    SUCCESS = "SUCCESS"

@celery_app.task(bind=True, max_retries=3)
def run_task(self, grid_id: str, x: int, y: int):
    """Simulates task execution with random outcomes."""
    try:
        r = random.random()
        if r < 0.10:
            raise self.retry(countdown=2)
        elif r < 0.10 + 0.001:
            return {"grid_id": grid_id, "x": x, "y": y, "status": Outcome.NON_RETRIABLE_ERROR}
        else:
            time.sleep(random.uniform(1, 5))
            return {"grid_id": grid_id, "x": x, "y": y, "status": Outcome.SUCCESS}
    except Exception:
        return {"grid_id": grid_id, "x": x, "y": y, "status": Outcome.RETRIABLE_ERROR}

@app.post("/create_grid/")
def create_grid(grid_id: str, m: int, n: int):
    """Creates a grid and dispatches tasks."""
    for i in range(m):
        for j in range(n):
            run_task.delay(grid_id, i, j)
    return {"message": "Grid created and tasks started", "grid_id": grid_id}

@app.get("/grid_status/")
def grid_status(grid_id: str):
    """Fetch grid task progress from Redis."""
    # Implement Redis-based storage for task tracking
    return {"grid_id": grid_id, "status": "In Progress"}