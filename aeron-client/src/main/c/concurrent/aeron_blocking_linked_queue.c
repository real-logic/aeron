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

#include "concurrent/aeron_blocking_linked_queue.h"

int aeron_blocking_linked_queue_init(aeron_blocking_linked_queue_t *queue)
{
    if (aeron_spsc_concurrent_linked_queue_init(&queue->spsc_queue) < 0)
    {
        return -1;
    }

    queue->size = 0;
    aeron_mutex_init(&queue->mutex, NULL);
    aeron_cond_init(&queue->cv, NULL);

    return 0;
}

int aeron_blocking_linked_queue_close(aeron_blocking_linked_queue_t *queue)
{
    aeron_mutex_lock(&queue->mutex);

    if (0 != queue->size)
    {
        aeron_mutex_unlock(&queue->mutex);

        // can't delete the queue until it's empty
        return -1;
    }

    if (aeron_spsc_concurrent_linked_queue_close(&queue->spsc_queue) < 0)
    {
        return -1;
    }

    aeron_mutex_unlock(&queue->mutex);

    aeron_mutex_destroy(&queue->mutex);
    aeron_cond_destroy(&queue->cv);

    return 0;
}

int aeron_blocking_linked_queue_offer(aeron_blocking_linked_queue_t *queue, void *element)
{
    int rc;

    aeron_mutex_lock(&queue->mutex);

    rc = aeron_spsc_concurrent_linked_queue_offer(&queue->spsc_queue, element);

    if (rc == 0)
    {
        queue->size++;
        aeron_cond_signal(&queue->cv);
    }

    aeron_mutex_unlock(&queue->mutex);

    return rc;
}

void *aeron_blocking_linked_queue_poll(aeron_blocking_linked_queue_t *queue)
{
    void *element = NULL;

    aeron_mutex_lock(&queue->mutex);

    if (0 == queue->size)
    {
        aeron_cond_wait(&queue->cv, &queue->mutex);
    }

    element = aeron_spsc_concurrent_linked_queue_poll(&queue->spsc_queue);

    if (NULL != element)
    {
        queue->size--;
    }

    aeron_mutex_unlock(&queue->mutex);

    return element;
}

size_t aeron_blocking_linked_queue_size(aeron_blocking_linked_queue_t *queue)
{
    size_t size;

    aeron_mutex_lock(&queue->mutex);

    size = queue->size;

    aeron_mutex_unlock(&queue->mutex);

    return size;
}

void aeron_blocking_linked_queue_unblock(aeron_blocking_linked_queue_t *queue)
{
    aeron_cond_signal(&queue->cv);
}
