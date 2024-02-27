/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sys/errno.h>
#include "aeron_executor.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_atomic.h"

struct aeron_executor_task_stct
{
    aeron_executor_t *executor;
    aeron_executor_task_on_execute_func_t on_execute;
    aeron_executor_task_on_complete_func_t on_complete;
    void *clientd;
    int result;
    aeron_linked_queue_node_t *queue_node;
    bool shutdown;
};

#define EXECUTOR_IS_ASYNC(_executor) (NULL != (_executor)->on_execution_complete)

aeron_executor_task_t *aeron_executor_task_acquire(
    aeron_executor_t *executor,
    aeron_executor_task_on_execute_func_t on_execute,
    aeron_executor_task_on_complete_func_t on_complete,
    void *clientd,
    bool shutdown)
{
    aeron_executor_task_t *task;

    if (aeron_alloc((void **)&task, sizeof(aeron_executor_task_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return NULL;
    }

    task->executor = executor;
    task->on_execute = on_execute;
    task->on_complete = on_complete;
    task->clientd = clientd;
    task->result = -1;
    task->queue_node = NULL;
    task->shutdown = shutdown;

    return task;
}

void aeron_executor_task_release(aeron_executor_task_t *task)
{
    aeron_linked_queue_node_delete(task->queue_node);
    aeron_free(task);
}

static void *aeron_executor_dispatch(void *arg)
{
    aeron_executor_t *executor = (aeron_executor_t *)arg;

    aeron_thread_set_name("aeron_executor");

    aeron_executor_task_t *task;
    aeron_linked_queue_node_t *node;

    while (true)
    {
        task = (aeron_executor_task_t *)aeron_blocking_linked_queue_take_ex(&executor->queue, &node);

        if (NULL == task)
        {
            continue;
        }

        task->queue_node = node;

        if (task->shutdown)
        {
            aeron_executor_task_release(task);
            break;
        }

        task->result = task->on_execute(task->clientd, executor->clientd);

        // TODO if result is < 0, check the AERON_SET_ERR stuff?

        // TODO check result of the following function as well?
        executor->on_execution_complete(task, executor->clientd);
    }

    return NULL;
}

/*
 * We could, at some point in the future, extend the executor to (optionally) include
 * a 'return queue' onto which dispatch_thread enqueues tasks that have had their
 * execute function called, and then a 'do_work' function that handles the completion
 * of those tasks.
 */
int aeron_executor_init(
    aeron_executor_t *executor,
    aeron_executor_on_execution_complete_func_t on_execution_complete,
    void *clientd)
{
    executor->on_execution_complete = on_execution_complete;
    executor->clientd = clientd;

    if (EXECUTOR_IS_ASYNC(executor))
    {
        if (aeron_blocking_linked_queue_init(&executor->queue) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        pthread_attr_t attr;
        int result;
        if ((result = aeron_thread_attr_init(&attr)) != 0)
        {
            AERON_SET_ERR(result, "%s", "aeron_thread_attr_init failed");
            return -1;
        }

        if ((result = aeron_thread_create(&executor->dispatch_thread, &attr, aeron_executor_dispatch, executor)) != 0)
        {
            AERON_SET_ERR(result, "%s", "aeron_thread_create failed");
            return -1;
        }
    }

    return 0;
}

int aeron_executor_close(aeron_executor_t *executor)
{
    if (EXECUTOR_IS_ASYNC(executor))
    {
        aeron_executor_task_t *task;

        // enqueue a task with shutdown = true
        task = aeron_executor_task_acquire(executor, NULL, NULL, NULL, true);
        if (NULL == task)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        if (aeron_blocking_linked_queue_offer(&executor->queue, task) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        int result = aeron_thread_join(executor->dispatch_thread, NULL);
        if (0 != result)
        {
            AERON_SET_ERR(result, "aeron_thread_join: %s", strerror(result));
            return -1;
        }

        if (aeron_blocking_linked_queue_close(&executor->queue) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    return 0;
}

int aeron_executor_submit(
    aeron_executor_t *executor,
    aeron_executor_task_on_execute_func_t on_execute,
    aeron_executor_task_on_complete_func_t on_complete,
    void *clientd)
{
    int result;

    if (EXECUTOR_IS_ASYNC(executor))
    {
        aeron_executor_task_t *task;

        task = aeron_executor_task_acquire(executor, on_execute, on_complete, clientd, false);
        if (NULL == task)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        result = aeron_blocking_linked_queue_offer(&executor->queue, task);
    }
    else
    {
        result = on_execute(clientd, executor->clientd);

        // TODO if result is < 0, check the AERON_SET_ERR stuff?

        result = on_complete(result, clientd, executor->clientd);
    }

    return result;
}

int aeron_executor_task_do_complete(aeron_executor_task_t *task)
{
    int result = task->on_complete(task->result, task->clientd, task->executor->clientd);

    // TODO if result is < 0, check the AERON_SET_ERR stuff?  Or should that be done by the caller?

    aeron_executor_task_release(task);

    return result;
}
