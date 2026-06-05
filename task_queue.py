"""A FIFO task queue backed by a DuckDB table.

DuckDB is an embedded database (like SQLite), not a server, so there is no
blocking pop or pub/sub. Instead the queue is just a table, and the pattern is:

    enqueue  ->  INSERT a row with status 'pending'
    dequeue  ->  atomically CLAIM the oldest pending row:
                 UPDATE ... SET status='processing' ... RETURNING
    complete ->  UPDATE that row's status to 'done'

Each task therefore moves through three states:  pending -> processing -> done.
Ordering by an auto-incrementing row id gives FIFO (oldest pending runs next).

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

# Where the DuckDB database file lives. Override with DUCKDB_PATH.
DB_PATH = os.environ.get("DUCKDB_PATH", "tasks.duckdb")


class TaskQueue:
    """A FIFO queue of JSON tasks stored in a DuckDB table."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        # One shared connection, guarded by a lock (see module docstring).
        self.conn = duckdb.connect(db_path)
        self.lock = threading.Lock()
        self._create_schema()

    def _create_schema(self) -> None:
        with self.lock:
            # A sequence supplies the auto-incrementing row id used for FIFO order.
            self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_row START 1")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    row_id     BIGINT PRIMARY KEY DEFAULT nextval('seq_row'),
                    payload    VARCHAR NOT NULL,                    -- JSON task dict
                    status     VARCHAR NOT NULL DEFAULT 'pending',  -- pending|processing|done
                    created_at TIMESTAMP DEFAULT now()
                )
                """
            )

    def enqueue(self, task: dict) -> None:
        """Add a task (a plain dict) to the back of the queue."""
        with self.lock:
            self.conn.execute(
                "INSERT INTO tasks (payload) VALUES (?)", [json.dumps(task)]
            )

    def dequeue(self, timeout: int = 2):
        """Claim and return the oldest pending task as a dict.

        DuckDB has no blocking pop, so we poll: we try to claim the oldest
        pending row; if there is none we wait briefly and try again, until
        `timeout` seconds have passed, at which point we return None. Returning
        None lets a worker re-check whether it has been asked to shut down.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self.lock:
                # Atomically grab the oldest pending row and mark it 'processing'.
                row = self.conn.execute(
                    """
                    UPDATE tasks SET status = 'processing'
                    WHERE row_id = (
                        SELECT row_id FROM tasks
                        WHERE status = 'pending'
                        ORDER BY row_id
                        LIMIT 1
                    )
                    RETURNING row_id, payload
                    """
                ).fetchone()
            if row is not None:
                row_id, payload = row
                task = json.loads(payload)
                # Remember which DB row this came from so complete() can find it.
                task["_row_id"] = row_id
                return task
            if time.monotonic() >= deadline:
                return None
            time.sleep(0.1)  # nothing waiting; pause before polling again

    def complete(self, task: dict) -> None:
        """Mark a claimed task as done."""
        with self.lock:
            self.conn.execute(
                "UPDATE tasks SET status = 'done' WHERE row_id = ?",
                [task["_row_id"]],
            )

    def size(self) -> int:
        """How many tasks are not yet done (pending + processing)."""
        with self.lock:
            (count,) = self.conn.execute(
                "SELECT count(*) FROM tasks WHERE status <> 'done'"
            ).fetchone()
        return count

    def stats(self) -> dict:
        """Return a {status: count} breakdown, handy for a final summary."""
        with self.lock:
            rows = self.conn.execute(
                "SELECT status, count(*) FROM tasks GROUP BY status"
            ).fetchall()
        return {status: count for status, count in rows}

    def clear(self) -> None:
        """Remove all tasks so a fresh demo run starts clean."""
        with self.lock:
            self.conn.execute("DELETE FROM tasks")
