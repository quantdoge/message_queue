# DuckDB Task Queue Demo

A small, easy-to-read demonstration of the classic **task queue** pattern, using
**DuckDB** (an embedded database) as the store:

```
producer  --INSERT-->  [ tasks table in DuckDB ]  --CLAIM-->  background workers
```

A **producer** inserts tasks into a table, and one or more **background workers**
claim pending rows and process them. The queue is persisted to a DuckDB file.

## How DuckDB is used

DuckDB is embedded (like SQLite), not a server — there is no blocking pop or
pub/sub. So the queue is just a table, and each task moves through three states:

```
pending  --claimed by a worker-->  processing  --finished-->  done
```

| Action   | SQL | Why |
|----------|-----|-----|
| enqueue  | `INSERT` a row with status `pending` | add work |
| dequeue  | `UPDATE ... SET status='processing' WHERE row_id = (oldest pending) RETURNING ...` | atomically **claim** one task |
| complete | `UPDATE ... SET status='done'` | mark it finished |
| size     | `SELECT count(*) ... WHERE status <> 'done'` | how much work is left |

Ordering by an auto-incrementing `row_id` gives **FIFO** (oldest pending runs
next). There's no blocking pop, so workers **poll**: claim the next task, or wait
briefly and try again.

### Important: one writer process at a time

DuckDB lets only **one process** open the database file read-write at a time.
So unlike a Redis broker, you can't scale workers across separate
processes/containers on the same file. The demo instead runs as **one process
with several worker threads** sharing a single connection (guarded by a lock).
The DB calls are fast and serialized; the simulated *work* still overlaps across
threads, so you still see real concurrency.

## Files

| File             | What it is |
|------------------|------------|
| `task_queue.py`  | `TaskQueue` class — `enqueue` / `dequeue` (claim) / `complete` / `size` over a DuckDB table |
| `worker.py`      | `Worker` class — the loop that claims tasks, processes them, marks them done |
| `producer.py`    | makes up sample tasks and inserts them |
| `demo.py`        | runs producer + several worker threads together in one process |

## Running it locally

### 1. Install the dependency

```bash
pip install -r requirements.txt
```

### 2. Run the demo

```bash
python demo.py
```

You'll see the workers' `processing` / `done` lines interleave, then a final
breakdown like `{'done': 10}`. The queue lives in `tasks.duckdb`.

### 3. Inspect the persisted queue with SQL

```bash
duckdb tasks.duckdb "SELECT status, count(*) FROM tasks GROUP BY status"
duckdb tasks.duckdb "SELECT * FROM tasks LIMIT 5"
```

(Point the demo at a different file with `export DUCKDB_PATH=/path/to/my.duckdb`.)

## Running it with Docker

DuckDB needs no server, so the whole demo runs in a single container:

```bash
docker compose up --build
```

The DuckDB file is written to `./data/tasks.duckdb` on the host (via a mounted
volume), so it persists after the container exits. Clean up with:

```bash
docker compose down
```
