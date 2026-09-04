#ifndef DISPATCHQUEUE_H
#define DISPATCHQUEUE_H

#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>

#define error_exit(MESSAGE) perror(MESSAGE), exit(EXIT_FAILURE)

typedef enum { ASYNC, SYNC } task_dispatch_type_t;
typedef enum { CONCURRENT, SERIAL } queue_type_t;

struct dispatch_queue_t;
typedef struct dispatch_queue_t dispatch_queue_t;

typedef struct task {
    char name[64];
    void (*work)(void *);
    void *params;
    task_dispatch_type_t type;
    sem_t task_semaphore;
} task_t;

typedef struct task_node {
    task_t *task;
    struct task_node *next;
} task_node_t;

typedef struct dispatch_queue_thread {
    dispatch_queue_t *queue;
    pthread_t thread;
} dispatch_queue_thread_t;

struct dispatch_queue_t {
    queue_type_t queue_type; // SERIAL or CONCURRENT
    dispatch_queue_thread_t *threads; // the worker threads
    int thread_count; // 1 for SERIAL, num cores for CONCURRENT
    task_node_t *head;  // front of linked list (next to run)
    task_node_t *tail;  // back of linked list (newest task)
    pthread_mutex_t mutex; // protects linked list from corruption
    pthread_cond_t all_done;   // what dispatch_queue_wait sleeps on
    sem_t task_semaphore;   // workers sleep on this, posted when task added
    int running;  // 1 = alive, 0 = shutting down
    int accepting_tasks;  // 1 = taking tasks, 0 = after wait called
    int tasks_in_progress;  // how many workers are currently running a task
};

task_t *task_create(void (*)(void *), void *, char *);
void task_destroy(task_t *);
dispatch_queue_t *dispatch_queue_create(queue_type_t);
void dispatch_queue_destroy(dispatch_queue_t *);
void dispatch_async(dispatch_queue_t *, task_t *);
void dispatch_sync(dispatch_queue_t *, task_t *);
void dispatch_queue_wait(dispatch_queue_t *);

#endif