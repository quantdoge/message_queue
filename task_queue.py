"""A tiny FIFO task queue backed by a Redis list.

The whole idea fits in three Redis commands:

    enqueue  ->  LPUSH   (push a task onto the LEFT of the list)
    dequeue  ->  BRPOP   (blocking pop from the RIGHT of the list)
    size     ->  LLEN    (how many tasks are waiting)

Pushing on the left and popping from the right gives us FIFO order:
the oldest task is always the next one taken. BRPOP *blocks* until a task
is available, so workers can sleep cheaply instead of busy-looping.
"""

import json
import os

import redis

# Where to find Redis. Defaults to a local server; override with REDIS_URL.
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# The name of the Redis list we use as the queue.
DEFAULT_QUEUE_KEY = "tasks"


class TaskQueue:
    """A FIFO queue of JSON tasks stored in a Redis list."""

    def __init__(self, key=DEFAULT_QUEUE_KEY, url=REDIS_URL):
        self.key = key
        # decode_responses=True so Redis gives us str instead of bytes.
        self.redis = redis.Redis.from_url(url, decode_responses=True)

    def enqueue(self, task: dict) -> None:
        """Add a task (a plain dict) to the back of the queue."""
        self.redis.lpush(self.key, json.dumps(task))

    def dequeue(self, timeout: int = 5):
        """Block until a task is available, then return it as a dict.

        Returns None if no task arrived within `timeout` seconds, which lets
        a worker periodically check whether it has been asked to shut down.
        """
        result = self.redis.brpop(self.key, timeout=timeout)
        if result is None:
            return None
        # BRPOP returns a (queue_key, value) tuple; we only want the value.
        _key, raw_task = result
        return json.loads(raw_task)

    def size(self) -> int:
        """How many tasks are currently waiting in the queue."""
        return self.redis.llen(self.key)
