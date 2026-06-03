# Redis Task Queue Demo

A small, easy-to-read demonstration of the classic **task queue** pattern:

```
producer  --enqueue-->  [ Redis list ]  --dequeue-->  background workers
```

A **producer** drops tasks onto a queue, and one or more **background workers**
pull tasks off the queue and process them. Redis stores the queue.

## How Redis is used

The queue is a single Redis **list**, and the whole pattern is three commands:

| Action  | Redis command | Why |
|---------|---------------|-----|
| enqueue | `LPUSH`       | push a task onto the **left** of the list |
| dequeue | `BRPOP`       | **blocking** pop from the **right** of the list |
| size    | `LLEN`        | how many tasks are waiting |

Pushing on the left and popping from the right gives **FIFO** order (oldest task
runs next). `BRPOP` blocks until a task exists, so idle workers sleep instead of
busy-looping. Because every worker pops from the same list, Redis hands each task
to exactly one worker — that is how the work is shared.

## Files

| File             | What it is |
|------------------|------------|
| `task_queue.py`  | `TaskQueue` class — the `enqueue` / `dequeue` / `size` wrapper around Redis |
| `worker.py`      | `Worker` class — the loop that pops tasks and processes them |
| `producer.py`    | makes up sample tasks and enqueues them |
| `demo.py`        | runs producer + several workers together in one process |

## Running it

### 1. Start Redis

```bash
docker compose up -d
```

(or point `REDIS_URL` at any Redis you already have, e.g.
`export REDIS_URL=redis://localhost:6379/0`)

### 2. Install the dependency

```bash
pip install -r requirements.txt
```

### 3a. Quick all-in-one demo

```bash
python demo.py
```

You'll see the workers' `processing` / `done` lines interleave, then
`All tasks processed.`

### 3b. Realistic multi-process version

Open three terminals:

```bash
python worker.py     # terminal 1 — a worker, waits for tasks
python worker.py     # terminal 2 — another worker
python producer.py   # terminal 3 — drops 10 tasks on the queue
```

Watch the two workers split the tasks between them. Stop a worker with `Ctrl-C`.

### Peek at the queue

```bash
redis-cli LLEN tasks   # number of tasks waiting
```
