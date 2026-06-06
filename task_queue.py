"""A FIFO task queue backed by a DuckDB table.

DuckDB is an embedded database (like SQLite), not a server, so there is no
blocking pop or pub/sub. Instead the queue is just a table, and the pattern is:

    enqueue      ->  INSERT a row with status 'queued'
    dequeue      ->  atomically CLAIM the oldest queued row:
                     UPDATE ... SET status='processing' ... RETURNING
    succeed      ->  UPDATE that row's status to 'success', store result JSON
    fail         ->  UPDATE that row's status to 'failed', store error message

Each task moves through:  queued -> processing -> success | failed

Concurrency note: a single DuckDB connection is not safe to use from several
threads at once, so every database call here is wrapped in a threading.Lock.
The lock is held only for the (fast) SQL itself -- never while a worker is
sleeping or doing work -- so workers still process tasks concurrently.
"""

import json
import os
import threading
import time

import duckdb

DB_PATH = os.environ.get("DUCKDB_PATH", "tasks.duckdb")


class TaskQueue:
    """A FIFO queue of JSON tasks stored in a DuckDB table."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.lock = threading.Lock()
        self._create_schema()

    def _create_schema(self) -> None:
        with self.lock:
            self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_row START 1")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    row_id     BIGINT PRIMARY KEY DEFAULT nextval('seq_row'),
                    payload    VARCHAR NOT NULL,
                    status     VARCHAR NOT NULL DEFAULT 'queued',
                    result     VARCHAR,
                    error      VARCHAR,
                    created_at TIMESTAMP DEFAULT now(),
                    updated_at TIMESTAMP DEFAULT now()
                )
                """
            )
            # Migrate tables created before result/error/updated_at columns existed
            for col, defn in [
                ("result", "VARCHAR"),
                ("error", "VARCHAR"),
                ("updated_at", "TIMESTAMP DEFAULT now()"),
            ]:
                try:
                    self.conn.execute(
                        f"ALTER TABLE tasks ADD COLUMN IF NOT EXISTS {col} {defn}"
                    )
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Enqueue helpers
    # ------------------------------------------------------------------

    def enqueue(self, task: dict) -> int:
        """Add a task dict to the queue. Returns the assigned ticket id."""
        with self.lock:
            (row_id,) = self.conn.execute(
                "INSERT INTO tasks (payload) VALUES (?) RETURNING row_id",
                [json.dumps(task)],
            ).fetchone()
        return row_id

    def enqueue_file(self, filename: str, file_data_b64: str) -> int:
        """Enqueue a file upload clustering task. Returns the ticket id."""
        payload = {"filename": filename, "file_data": file_data_b64}
        return self.enqueue(payload)

    # ------------------------------------------------------------------
    # Worker-facing methods
    # ------------------------------------------------------------------

    def dequeue(self, timeout: int = 2):
        """Claim and return the oldest queued task as a dict, or None on timeout."""
        deadline = time.monotonic() + timeout
        while True:
            with self.lock:
                row = self.conn.execute(
                    """
                    UPDATE tasks
                    SET    status     = 'processing',
                           updated_at = now()
                    WHERE  row_id = (
                        SELECT row_id FROM tasks
                        WHERE  status = 'queued'
                        ORDER  BY row_id
                        LIMIT  1
                    )
                    RETURNING row_id, payload
                    """
                ).fetchone()
            if row is not None:
                row_id, payload = row
                task = json.loads(payload)
                task["_row_id"] = row_id
                return task
            if time.monotonic() >= deadline:
                return None
            time.sleep(0.1)

    def succeed(self, task: dict, result: str) -> None:
        """Mark a task as successfully completed and store the result JSON."""
        with self.lock:
            self.conn.execute(
                "UPDATE tasks SET status = 'success', result = ?, updated_at = now() WHERE row_id = ?",
                [result, task["_row_id"]],
            )

    def fail(self, task: dict, error: str) -> None:
        """Mark a task as failed and store the error message."""
        with self.lock:
            self.conn.execute(
                "UPDATE tasks SET status = 'failed', error = ?, updated_at = now() WHERE row_id = ?",
                [error, task["_row_id"]],
            )

    def complete(self, task: dict) -> None:
        """Backward-compatible alias: mark done with no result."""
        self.succeed(task, result="")

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_task(self, ticket_id: int) -> dict | None:
        """Return status (and result/error if finished) for a ticket, or None."""
        with self.lock:
            row = self.conn.execute(
                "SELECT row_id, status, result, error FROM tasks WHERE row_id = ?",
                [ticket_id],
            ).fetchone()
        if row is None:
            return None
        row_id, status, result, error = row
        out = {"ticket_id": row_id, "status": status}
        if status == "success":
            out["result"] = json.loads(result) if result else []
        if status == "failed":
            out["error"] = error
        return out

    def size(self) -> int:
        """How many tasks are not yet finished (queued + processing)."""
        with self.lock:
            (count,) = self.conn.execute(
                "SELECT count(*) FROM tasks WHERE status NOT IN ('success', 'failed')"
            ).fetchone()
        return count

    def stats(self) -> dict:
        """Return a {status: count} breakdown."""
        with self.lock:
            rows = self.conn.execute(
                "SELECT status, count(*) FROM tasks GROUP BY status"
            ).fetchall()
        return {status: count for status, count in rows}

    def clear(self) -> None:
        """Remove all tasks."""
        with self.lock:
            self.conn.execute("DELETE FROM tasks")
