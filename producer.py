"""Produces sample tasks and puts them on the queue.

In a real system this would be your web app, cron job, or another service
saying "please do this work later". Here we just make up a few jobs.
"""

import random

from task_queue import TaskQueue

# A few made-up task types so the output is interesting to watch.
TASK_TYPES = ["send_email", "resize_image", "generate_report", "charge_card"]


def enqueue_sample_tasks(queue: TaskQueue, n: int = 10) -> None:
    """Push `n` example tasks onto the queue."""
    for i in range(1, n + 1):
        task = {
            "id": i,
            "type": random.choice(TASK_TYPES),
            # How long this task will pretend to take, in seconds.
            "duration": random.randint(1, 3),
        }
        queue.enqueue(task)
        print(f"[producer] enqueued task {task['id']} ({task['type']})")
    print(f"[producer] enqueued {n} tasks; queue size is now {queue.size()}")


if __name__ == "__main__":
    enqueue_sample_tasks(TaskQueue(), n=10)
