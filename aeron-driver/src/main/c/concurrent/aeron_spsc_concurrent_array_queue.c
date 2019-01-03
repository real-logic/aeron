/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "aeron_alloc.h"
#include "concurrent/aeron_spsc_concurrent_array_queue.h"

int aeron_spsc_concurrent_array_queue_init(
    volatile aeron_spsc_concurrent_array_queue_t *queue, uint64_t length)
{
    length = (uint64_t)aeron_find_next_power_of_two((int32_t)length);

    if (aeron_alloc((void **)&queue->buffer, sizeof(void *) * length) < 0)
    {
        return -1;
    }

    for (size_t i = 0; i < length; i++)
    {
        queue->buffer[i] = NULL;
    }

    queue->producer.head_cache = 0;
    queue->producer.tail = 0;
    queue->consumer.head = 0;
    queue->capacity = length;
    queue->mask = length - 1;
    return 0;
}

int aeron_spsc_concurrent_array_queue_close(aeron_spsc_concurrent_array_queue_t *queue)
{
    aeron_free(queue->buffer);
    return 0;
}

extern aeron_queue_offer_result_t aeron_spsc_concurrent_array_queue_offer(
    volatile aeron_spsc_concurrent_array_queue_t *queue,
    void *element);

extern uint64_t aeron_spsc_concurrent_array_queue_drain(
    volatile aeron_spsc_concurrent_array_queue_t *queue,
    aeron_queue_drain_func_t func,
    void *clientd,
    uint64_t limit);

extern uint64_t aeron_spsc_concurrent_array_queue_drain_all(
    volatile aeron_spsc_concurrent_array_queue_t *queue,
    aeron_queue_drain_func_t func,
    void *clientd);

extern uint64_t aeron_spsc_concurrent_array_queue_size(volatile aeron_spsc_concurrent_array_queue_t *queue);
