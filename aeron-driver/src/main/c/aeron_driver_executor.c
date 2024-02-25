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

#include "aeron_driver_executor.h"
#include "aeron_alloc.h"

struct aeron_driver_executor_task_stct
{
    aeron_driver_executor_t *executor;
    aeron_driver_executor_task_on_execute_func_t on_execute;
    aeron_driver_executor_task_on_complete_func_t on_complete;
    void *clientd;
    int result;
};

#define AERON_DRIVER_EXECUTOR_IS_ASYNC(_executor) (NULL != (_executor)->on_execution_complete)

int aeron_driver_executor_init(
    aeron_driver_executor_t *executor,
    aeron_driver_executor_on_execution_complete_func_t on_execution_complete,
    void *clientd)
{
    if (aeron_blocking_linked_queue_init(&executor->queue) < 0)
    {
        return -1;
    }

    executor->on_execution_complete = on_execution_complete;
    executor->clientd = clientd;

    return 0;
}

int aeron_driver_executor_close(aeron_driver_executor_t *executor)
{
    aeron_blocking_linked_queue_close(&executor->queue);

    return 0;
}

int aeron_driver_executor_do_work(aeron_driver_executor_t *executor)
{
    if (AERON_DRIVER_EXECUTOR_IS_ASYNC(executor))
    {
        aeron_driver_executor_task_t *task;
        int count = 0;

        while (NULL != (task = (aeron_driver_executor_task_t *)aeron_blocking_linked_queue_poll(&executor->queue)))
        {
            task->result = task->on_execute(task->clientd, executor->clientd);

            // TODO if result is < 0, check the AERON_SET_ERR stuff?

            // TODO check result of the following function as well?
            executor->on_execution_complete(task, executor->clientd);

            count++;
        }

        return count;
    }
    else
    {
        return 0;
    }
}

int aeron_driver_executor_execute(
    aeron_driver_executor_t *executor,
    aeron_driver_executor_task_on_execute_func_t on_execute,
    aeron_driver_executor_task_on_complete_func_t on_complete,
    void *clientd)
{
    int result;

    if (AERON_DRIVER_EXECUTOR_IS_ASYNC(executor))
    {
        aeron_driver_executor_task_t *task;

        if (aeron_alloc((void **)&task, sizeof(aeron_driver_executor_task_t)) < 0)
        {
            return -1;
        }

        task->executor = executor;
        task->on_execute = on_execute;
        task->on_complete = on_complete;
        task->clientd = clientd;
        task->result = -1;

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

int aeron_driver_executor_task_do_complete(aeron_driver_executor_task_t *task)
{
    /*
     * TODO in a glorious future where we want to have multiple dispatch threads but we also want to maintain
     * the ordering of processing the on_complete functions, we could keep track of the tasks we're asked to
     * execute in some FIFO queue, and in here just mark a task as 'ready', then check the FIFO queue to see
     * if the next task is 'ready'.  Then dequeue in order until we run out of 'ready' tasks.
     */

    int result = task->on_complete(task->result, task->clientd, task->executor->clientd);

    // TODO if result is < 0, check the AERON_SET_ERR stuff?  Or should that be done by the caller?

    aeron_free(task);

    return result;
}
