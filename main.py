from fastapi import FastAPI, HTTPException
from flask import Flask, request, jsonify
from celery import Celery
import random
import time
import redis

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
def run_grid_task(self, grid_id: str, x: int, y: int):

    task_id = self.request.id  # Get Celery task ID
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

    return jsonify(grid_data)
    """return {"grid_id": grid_id, "status": "In Progress"}"""

@app.route("/kill-grid/<grid_id>", methods=["POST"])
def kill_grid(grid_id):
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

    return jsonify({"grid_id": grid_id, "killed_tasks": killed_tasks})

@app.route("/kill-job/<grid_id>/<int:i>/<int:j>", methods=["POST"])
def kill_job(grid_id, i, j):
    """
    Terminates a specific task running at (i, j) in grid_id.
    """
    redis_key = f"task_progress:{grid_id}:{i}:{j}"
    task_info = redis_client.hgetall(redis_key)
    task_id = task_info.get("task_id")

    if task_id:
        run_grid_task.AsyncResult(task_id).revoke(terminate=True)
        redis_client.set(f"kill:{task_id}", "1")  # Mark as killed
        redis_client.hset(redis_key, "status", "KILLED")
        return jsonify({"grid_id": grid_id, "cell": f"({i},{j})", "task_id": task_id, "status": "KILLED"})
    
    return jsonify({"error": "Task not found"}), 404