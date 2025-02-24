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

# Connect to Redis (storing job progress)
redis_client = redis.Redis(host="localhost", port=6379, db=2, decode_responses=True)

class Outcome:
    RETRIABLE_ERROR = "RETRIABLE_ERROR"
    NON_RETRIABLE_ERROR = "NON_RETRIABLE_ERROR"
    SUCCESS = "SUCCESS"

@celery_app.task(bind=True, max_retries=3)
def run_task(self, grid_id: str, x: int, y: int):

     redis_key = f"task_progress:{grid_id}:{i}:{j}"

    # Register the task in Redis
    redis_client.hset(redis_key, mapping={"status": "IN_PROGRESS", "progress": "0%", "task_id": task_id})


    """Simulates task execution with random outcomes."""
    try:
        for step in range(5):
            time.sleep(1)  # Simulate work
            progress = f"{(step+1) * 20}%"
            redis_client.hset(redis_key, "progress", progress)

            # Check if task termination has been requested
            if redis_client.exists(f"kill:{task_id}"):
                redis_client.hset(redis_key, "status", "KILLED")
                return "TASK_KILLED"

        r = random.random()
        if r < 0.10:
            """raise self.retry(countdown=2)"""
            redis_client.hset(redis_key, "status", "RETRIABLE_ERROR")
            return {"grid_id": grid_id, "x": x, "y": y, "status": Outcome.RETRIABLE_ERROR}
        elif r < 0.10 + 0.001:
             redis_client.hset(redis_key, "status", "NON_RETRIABLE_ERROR")
            return {"grid_id": grid_id, "x": x, "y": y, "status": Outcome.NON_RETRIABLE_ERROR}
        else:
            time.sleep(random.uniform(1, 5))
            redis_client.hset(redis_key, "status", "SUCCESS")
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