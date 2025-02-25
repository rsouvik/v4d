from fastapi import FastAPI, HTTPException
from flask import Flask, request, jsonify
from celery import Celery
import random
import time
import redis
import logging

app = FastAPI()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Log INFO and above (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),  # Log to a file
        logging.StreamHandler()  # Log to console
    ]
)

logger = logging.getLogger(__name__)  # Create logger instance

# Setup Celery
celery_app = Celery(
    "tasks",
    broker="redis://0.0.0.0:6379/0",
    backend="redis://0.0.0.0:6379/0"
)

# Connect to Redis (storing job progress)
redis_client = redis.Redis(host="0.0.0.0", port=6379, db=2, decode_responses=True)

class Outcome:
    RETRIABLE_ERROR = "RETRIABLE_ERROR"
    NON_RETRIABLE_ERROR = "NON_RETRIABLE_ERROR"
    SUCCESS = "SUCCESS"

@celery_app.task(bind=True, max_retries=3)
def run_grid_task(self, grid_id: str, x: int, y: int):

    task_id = self.request.id  # Get Celery task ID
    redis_key = f"task_progress:{grid_id}:{x}:{y}"

    # Register the task in Redis
    redis_client.hset(redis_key, mapping={"status": "IN_PROGRESS", "progress": "0%", "task_id": task_id})


    """Simulates task execution with random outcomes."""
    try:
        for step in range(5):
            time.sleep(60)  # Simulate work
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
            run_grid_task.delay(grid_id, i, j)
    return {"message": "Grid created and tasks started", "grid_id": grid_id}

@app.get("/grid_status/")
def grid_status(grid_id: str):
    """Fetch grid task progress from Redis."""
    # Implement Redis-based storage for task tracking
    keys = redis_client.keys(f"task_progress:{grid_id}:*")
    grid_data = {}

    for key in keys:
        _, _, i, j = key.split(":")  # Extract grid coordinates
        grid_data[f"({i},{j})"] = redis_client.hgetall(key)

    return {"grid_id": grid_id, "status": grid_data}
    """return {"grid_id": grid_id, "status": "In Progress"}"""

@app.post("/kill-grid/{grid_id}")
async def kill_grid(grid_id: str):
    """
    Terminates all tasks for a given grid_id.
    """
    keys = redis_client.keys(f"task_progress:{grid_id}:*")
    killed_tasks = []

    for key in keys:
        task_info = redis_client.hgetall(key)
        task_id = task_info.get("task_id")

        if task_id:
            run_grid_task.AsyncResult(task_id).revoke(terminate=True)
            redis_client.set(f"kill:{task_id}", "1")  # Mark as killed
            redis_client.hset(key, "status", "KILLED")
            killed_tasks.append(task_id)

    return JSONResponse(content={"grid_id": grid_id, "killed_tasks": killed_tasks})


@app.post("/kill-job/{grid_id}/{i}/{j}")
def kill_job(grid_id: str, i: int, j: int):
    """
    Terminates a specific task running at (i, j) in grid_id.
    """
    redis_key = f"task_progress:{grid_id}:{i}:{j}"
    logger.info(f"Killing task at {i}, {j} in grid {grid_id}")
    logger.info(f"Redis key: {redis_key}")
    task_info = redis_client.hgetall(redis_key)
    logger.info(f"Task info: {task_info}")
    task_id = task_info.get("task_id")
    logger.info(f"Task ID: {task_id}")

    if task_id:
        run_grid_task.AsyncResult(task_id).revoke(terminate=True)
        redis_client.set(f"kill:{task_id}", "1")  # Mark as killed
        redis_client.hset(redis_key, "status", "KILLED")
        return JSONResponse(content={"grid_id": grid_id, "cell": f"({i},{j})", "task_id": task_id, "status": "KILLED"})

    raise HTTPException(status_code=404, detail="Task not found")

@app.get("/redis-health")
async def redis_health():
    """
    Checks if Redis is reachable and operational.
    """
    try:
        # Try to set and get a test key
        redis_client.set("health_check", "ok", ex=10)  # Expires in 10 seconds
        value = redis_client.get("health_check")

        if value == b"ok":
            return {"status": "Redis is running and reachable"}
        else:
            return {"status": "Redis is not responding correctly"}, 500
    except Exception as e:
        return {"status": "Redis connection failed", "error": str(e)}, 500
