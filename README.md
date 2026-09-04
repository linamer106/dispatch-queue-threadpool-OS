# Thread Pools and Dispatch Queues

## Description

This project implements a small subset of a dispatch-queue-based thread pool
system in C, similar in spirit to Apple's Grand Central Dispatch. Instead of
manually creating and destroying threads for every unit of work, tasks are
submitted to a **dispatch queue**, which hands them off to a small pool of
worker threads. Threads are created once and reused, avoiding the repeated
cost of thread creation/destruction.

Two kinds of queues are supported:

- **Serial queue** — runs one task at a time, in FIFO order. A task must
  finish before the next one starts. Useful for synchronizing access to
  shared state.
- **Concurrent queue** — runs multiple tasks in parallel (one pool thread
  per core), dispatching tasks in the order they were added, but allowing
  them to execute out of order relative to completion.

Tasks can be submitted **synchronously** (`dispatch_sync`, blocks until the
task completes) or **asynchronously** (`dispatch_async`, returns
immediately). A queue can also be waited on as a whole
(`dispatch_queue_wait`) until all currently-queued tasks finish; anything
added after the wait call is ignored.

## Files

| File              | Purpose                                                              |
|-------------------|-----------------------------------------------------------------------|
| `num_cores.c`     | Prints the number of physical cores on the machine.                  |
| `dispatchQueue.h` | Type definitions for tasks and dispatch queues.                      |
| `dispatchQueue.c` | Implementation of the dispatch queue / thread pool library.          |
| `Full_Report.pdf`   | Task reflection.  |

## Public API (`dispatchQueue.h` / `dispatchQueue.c`)

```c
dispatch_queue_t *dispatch_queue_create(queue_type_t queueType);
void               dispatch_queue_destroy(dispatch_queue_t *queue);

task_t *task_create(void (*work)(void *), void *param, char *name);
void     task_destroy(task_t *task);

void dispatch_sync(dispatch_queue_t *queue, task_t *task);
void dispatch_async(dispatch_queue_t *queue, task_t *task);
void dispatch_queue_wait(dispatch_queue_t *queue);
```

- `queueType` is either `CONCURRENT` or `SERIAL`.
- Concurrent queues spin up one worker thread per core (as reported by
  `num_cores.c` / matching the Gnome System Monitor core count).
- Serial queues use a single worker thread.
- All queue and task functions print an error and exit on failure rather
  than returning an error code.
- Concurrency is protected using POSIX mutexes/semaphores
  (`pthread_mutex_t`, `sem_t`).

## Building

```bash
gcc num_cores.c -o num_cores
gcc -pthread dispatchQueue.c testN.c -o testN   # where N is the test number
```

(A `Makefile` is provided for the test targets; run `make` to build them
all.)

## Running

```bash
./num_cores
# This machine has 2 cores.

./test1   # synchronous dispatch
./test2   # asynchronous dispatch
./test3   # asynchronous dispatch + wait
./test4   # concurrent queue — should drive all cores to ~100% (check System Monitor)
./test5   # serial queue — should mostly use a single core
```

## Implementation Details

### Core data structures

A dispatch queue is made up of two things: a linked list of waiting tasks,
and a pool of worker threads. Worker threads are created once when the
queue is created and sleep until a task arrives. The main thread adds
tasks to the linked list and wakes a worker up. The queue struct holds
everything needed to coordinate this:

- `queue_type` — `SERIAL` or `CONCURRENT`
- `threads` / `thread_count` — the worker pool (1 for `SERIAL`, one per
  core for `CONCURRENT`, via `sysconf(_SC_NPROCESSORS_ONLN)`)
- `head` / `tail` — a FIFO linked list of queued tasks
- `mutex` — protects the linked list from concurrent corruption
- `all_done` (condition variable) — what `dispatch_queue_wait` sleeps on
- `task_semaphore` — workers block on this; posted once per task added
- `running` / `accepting_tasks` / `tasks_in_progress` — queue lifecycle
  and completion tracking

### Worker threads

Every worker thread runs an infinite loop blocked on `task_semaphore`
(starts at 0). When a task is added, `sem_post` wakes exactly one sleeping
worker (or leaves the counter incremented if all workers are busy, so the
next one to finish picks it up immediately). On waking, a worker:

1. Locks the mutex and checks whether the queue is shutting down.
2. Dequeues the task at the head of the linked list (FIFO).
3. Increments `tasks_in_progress` and unlocks the mutex.
4. Runs `task->work(task->params)` outside the lock, so workers execute
   independently of each other.

New tasks always go to the tail; workers always take from the head, which
guarantees strict FIFO ordering regardless of queue type.

### Sync vs. async dispatch

Both `dispatch_async` and `dispatch_sync` enqueue onto the same linked
list using a shared helper — the difference is what happens afterwards,
controlled by two separate semaphores:

- **`queue->task_semaphore`** — shared by every task/worker on the queue.
  Incremented once per task added; acts as a counter of pending work.
- **`task->task_semaphore`** — private to a single sync task, created
  fresh in `dispatch_sync`. Only two threads ever touch it: the caller
  (which blocks on it) and the worker that runs the task (which posts to
  it when done).

`dispatch_async` enqueues the task, posts `task_semaphore`, and returns
immediately without waiting. `dispatch_sync` does the same but then blocks
on the task's own private semaphore until the worker signals completion,
at which point the caller (not the worker) owns and destroys the task.

After running a task, a worker checks its type: for `SYNC` tasks it posts
the task's private semaphore and leaves cleanup to the caller; for `ASYNC`
tasks, since nobody is waiting, the worker destroys the task itself. In
both cases the worker decrements `tasks_in_progress` and, once the queue
is both idle and empty, broadcasts `all_done` to wake any thread blocked
in `dispatch_queue_wait`.

### Serial vs. concurrent dispatching

- **Serial queues** have exactly one worker thread, so tasks can only run
  one at a time, strictly in FIFO order — task A finishes completely
  before task B is picked up. This is why `test5` always prints tasks in
  order A–J, the counter always reaches exactly 10,000,000,000, and only
  one CPU core is ever busy.
- **Concurrent queues** create one worker thread per core. All workers
  sleep on the same `task_semaphore` and race to dequeue; the mutex
  ensures only one dequeue happens at a time, but once dequeued, tasks run
  fully in parallel on separate cores. This is why `test4` drives all
  cores to 100% simultaneously, prints tasks in unpredictable order, and
  the shared counter rarely reaches 10,000,000,000 (concurrent,
  unsynchronized writes overwrite each other).

In summary: serial dispatches one task at a time through a single worker;
concurrent dispatches multiple tasks simultaneously through N workers.
Both share the same FIFO linked list and worker loop — the only
difference is how many workers are consuming from it.

## Documentation

The full written report — including detailed explanations and annotated
code snippets for how dispatching works — is available in
[`docs/Full_Report.pdf`](docs/A2.pdf).

## Notes

- Test source files are **not** included in the submission zip — only
  `num_cores.c`, `dispatchQueue.c`, `dispatchQueue.h`, and any additional
  files you write go into `A2.zip`.
- Written answers go in a separate `A2Answers.txt` or `A2Answers.pdf`,
  submitted alongside the zip via Canvas.
- Due 15 May 2026, 11:59pm.
