"""A background worker that pulls tasks off the queue and processes them.

Run several of these (in threads via demo.py, or as separate processes in
separate terminals) and they will each grab different tasks from the same
Redis queue -- that is how the work gets spread out.
"""

import threading
import time

from task_queue import TaskQueue


class Worker:
    """Pops tasks from a TaskQueue and 'processes' them until told to stop."""

    def __init__(self, name: str, queue: TaskQueue):
        self.name = name
        self.queue = queue
        # An Event is a simple thread-safe on/off flag we use to ask the
        # worker to finish up and exit its loop.
        self._stop = threading.Event()

    def stop(self) -> None:
        """Ask the worker to stop after it finishes its current wait/task."""
        self._stop.set()

    def run(self) -> None:
        """Main loop: wait for a task, process it, repeat."""
        print(f"[{self.name}] started, waiting for tasks...")
        while not self._stop.is_set():
            # Block up to 2s for a task. If none arrives we loop back and
            # re-check the stop flag, so shutdown is responsive.
            task = self.queue.dequeue(timeout=2)
            if task is None:
                continue
            self.process(task)
        print(f"[{self.name}] stopped.")

    def process(self, task: dict) -> None:
        """Pretend to do real work: sleep for the task's 'duration'."""
        duration = task.get("duration", 1)
        print(f"[{self.name}] processing task {task['id']} ({task['type']}, {duration}s)")
        time.sleep(duration)
        print(f"[{self.name}] done task {task['id']}")


if __name__ == "__main__":
    # Standalone mode: run one worker that keeps going until you Ctrl-C it.
    worker = Worker(name="worker-standalone", queue=TaskQueue())
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()
        print("\nInterrupted, shutting down.")
