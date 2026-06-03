"""One-command demonstration of the whole flow in a single process.

It:
  1. starts a few background workers (each in its own thread),
  2. enqueues a batch of tasks,
  3. waits for the queue to drain,
  4. asks the workers to stop, and joins them.

Watch the output: the workers' "processing"/"done" lines interleave, which
shows several tasks being handled at the same time.

For the more realistic multi-process version, run worker.py in a couple of
terminals and producer.py in another instead.
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

    # 1. Start the background workers.
    workers = [Worker(name=f"worker-{i}", queue=queue) for i in range(1, NUM_WORKERS + 1)]
    threads = [threading.Thread(target=w.run, daemon=True) for w in workers]
    for t in threads:
        t.start()

    # 2. Hand the workers some tasks to chew on.
    print("-" * 50)
    enqueue_sample_tasks(queue, n=NUM_TASKS)
    print("-" * 50)

    # 3. Wait until every task has been picked up off the queue.
    while queue.size() > 0:
        time.sleep(0.2)

    # 4. Tell the workers to stop, then wait for them to finish the last task.
    for w in workers:
        w.stop()
    for t in threads:
        t.join()

    print("-" * 50)
    print("All tasks processed. Demo complete.")


if __name__ == "__main__":
    main()
