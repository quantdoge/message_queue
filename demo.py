"""One-command demonstration of the whole flow in a single process.

It:
  1. starts a few background workers (each in its own thread),
  2. enqueues a batch of tasks,
  3. waits for the queue to drain,
  4. asks the workers to stop, and joins them,
  5. prints the final pending/processing/done breakdown.

Watch the output: the workers' "processing"/"done" lines interleave, which
shows several tasks being handled at the same time. Everything shares one
DuckDB connection, so this all runs as a single process (DuckDB only allows
one read-write process on the file at a time).
"""

import threading
import time

from producer import enqueue_sample_tasks
from task_queue import TaskQueue
from worker import Worker

NUM_WORKERS = 3
NUM_TASKS = 10


def main() -> None:
    queue = TaskQueue()
    queue.clear()  # start each run from a clean table

    # 1. Start the background workers.
    workers = [Worker(name=f"worker-{i}", queue=queue) for i in range(1, NUM_WORKERS + 1)]
    threads = [threading.Thread(target=w.run, daemon=True) for w in workers]
    for t in threads:
        t.start()

    # 2. Hand the workers some tasks to chew on.
    print("-" * 50)
    enqueue_sample_tasks(queue, n=NUM_TASKS)
    print("-" * 50)

    # 3. Wait until every task has been processed (pending + processing == 0).
    while queue.size() > 0:
        time.sleep(0.2)

    # 4. Tell the workers to stop, then wait for them to finish the last task.
    for w in workers:
        w.stop()
    for t in threads:
        t.join()

    # 5. Show the final state, which also lives in the DuckDB file on disk.
    print("-" * 50)
    print(f"All tasks processed. Final breakdown: {queue.stats()}")
    print(f"Inspect it any time with:  duckdb {queue.db_path} \"SELECT * FROM tasks\"")


if __name__ == "__main__":
    main()
