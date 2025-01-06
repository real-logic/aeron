/*
 * Copyright 2014-2025 Real Logic Limited.
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

#include "aeron_alloc.h"
#include "concurrent/aeron_mpsc_concurrent_array_queue.h"

int aeron_mpsc_concurrent_array_queue_init(aeron_mpsc_concurrent_array_queue_t *queue, size_t length)
{
    length = (size_t)aeron_find_next_power_of_two((int32_t)length);

    if (aeron_alloc((void **)&queue->buffer, sizeof(void *) * length) < 0)
    {
        return -1;
    }

    for (size_t i = 0; i < length; i++)
    {
        queue->buffer[i] = NULL;
    }

    queue->capacity = length;
    queue->mask = length - 1;
    queue->producer.head_cache = 0;
    queue->producer.shared_head_cache = 0;
    AERON_SET_RELEASE(queue->producer.tail, (uint64_t)0);
    AERON_SET_RELEASE(queue->consumer.head, (uint64_t)0);

    return 0;
}

int aeron_mpsc_concurrent_array_queue_close(aeron_mpsc_concurrent_array_queue_t *queue)
{
    aeron_free((void *)queue->buffer);
    return 0;
}

extern aeron_queue_offer_result_t aeron_mpsc_concurrent_array_queue_offer(
    aeron_mpsc_concurrent_array_queue_t *queue, void *element);

extern size_t aeron_mpsc_concurrent_array_queue_drain(
    aeron_mpsc_concurrent_array_queue_t *queue, aeron_queue_drain_func_t func, void *clientd, size_t limit);

extern size_t aeron_mpsc_concurrent_array_queue_drain_all(
    aeron_mpsc_concurrent_array_queue_t *queue, aeron_queue_drain_func_t func, void *clientd);

extern size_t aeron_mpsc_concurrent_array_queue_size(aeron_mpsc_concurrent_array_queue_t *queue);
