"""FastAPI application exposing two endpoints:

    POST /upload
        Accepts a CSV or Excel file, enqueues a DBSCAN clustering task,
        and returns a ticket id the caller can use to poll for results.

    GET /status/{ticket_id}
        Returns the current status (queued / processing / success / failed)
        and, once finished, the clustering result or error message.

Two ClusteringWorker threads are started in the background when the app
launches and share the same DuckDB-backed TaskQueue.
"""

import base64
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, UploadFile

from clustering_worker import ClusteringWorker
from task_queue import TaskQueue

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

queue = TaskQueue()
_workers: list[ClusteringWorker] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    for i in range(2):
        w = ClusteringWorker(name=f"clustering-worker-{i}", queue=queue)
        threading.Thread(target=w.run, daemon=True, name=f"worker-{i}").start()
        _workers.append(w)
    yield
    for w in _workers:
        w.stop()


app = FastAPI(title="Clustering Queue API", lifespan=lifespan)


@app.post("/upload", summary="Upload a CSV/Excel file to start a clustering task")
def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV or Excel file. The server enqueues a DBSCAN clustering job
    and returns a **ticket_id** you can use to track progress via `/status/{ticket_id}`.
    """
    name = file.filename or ""
    ext = ("." + name.rsplit(".", 1)[-1].lower()) if "." in name else ""
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{ext}'. Allowed: {sorted(ALLOWED_EXTENSIONS)}",
        )

    raw = file.file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    file_data_b64 = base64.b64encode(raw).decode()
    ticket_id = queue.enqueue_file(filename=name, file_data_b64=file_data_b64)

    return {"ticket_id": ticket_id, "status": "queued"}


@app.get("/status/{ticket_id}", summary="Check the status of a clustering task")
def get_status(ticket_id: int):
    """
    Poll the status of a previously submitted clustering task.

    Possible **status** values:
    - `queued`     — waiting in line
    - `processing` — a worker is currently running the job
    - `success`    — finished; `result` contains the cluster assignments
    - `failed`     — something went wrong; `error` contains the traceback
    """
    task = queue.get_task(ticket_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Ticket {ticket_id} not found.")
    return task
