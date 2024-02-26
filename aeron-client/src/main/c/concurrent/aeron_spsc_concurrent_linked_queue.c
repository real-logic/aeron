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

#include "aeron_alloc.h"
#include "concurrent/aeron_spsc_concurrent_linked_queue.h"
#include "util/aeron_error.h"

struct aeron_spsc_concurrent_linked_queue_node_stct {
    aeron_spsc_concurrent_linked_queue_node_t *next;
    void *element;
};

int aeron_spsc_concurrent_linked_queue_node_create(void *element, aeron_spsc_concurrent_linked_queue_node_t **nodep)
{
    aeron_spsc_concurrent_linked_queue_node_t *node;

    if (aeron_alloc((void **)&node, sizeof(aeron_spsc_concurrent_linked_queue_node_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    node->next = NULL;
    node->element = element;

    *nodep = node;

    return 0;
}

int aeron_spsc_concurrent_linked_queue_node_delete(aeron_spsc_concurrent_linked_queue_node_t *node)
{
    aeron_free(node);

    return 0;
}

int aeron_spsc_concurrent_linked_queue_init(aeron_spsc_concurrent_linked_queue_t *queue)
{
    aeron_spsc_concurrent_linked_queue_node_t *node;

    if (aeron_spsc_concurrent_linked_queue_node_create(NULL, &node))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    queue->head = node;
    queue->tail = node;

    return 0;
}

int aeron_spsc_concurrent_linked_queue_close(aeron_spsc_concurrent_linked_queue_t *queue)
{
    if (NULL == queue->tail->next)
    {
        // the queue is empty
        aeron_spsc_concurrent_linked_queue_node_delete(queue->tail);

        return 0;
    }

    // the queue is NOT empty
    return -1;
}

int aeron_spsc_concurrent_linked_queue_offer(aeron_spsc_concurrent_linked_queue_t *queue, void *element)
{
    aeron_spsc_concurrent_linked_queue_node_t *node, *prev;

    if (aeron_spsc_concurrent_linked_queue_node_create(element, &node))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    prev = queue->head;
    queue->head = node;
    prev->next = node;

    return 0;
}

void *aeron_spsc_concurrent_linked_queue_poll(aeron_spsc_concurrent_linked_queue_t *queue)
{
    aeron_spsc_concurrent_linked_queue_node_t *next;

    next = queue->tail->next;

    if (NULL != next)
    {
        aeron_spsc_concurrent_linked_queue_node_t *prev_tail;
        void *element;

        prev_tail = queue->tail;
        element = next->element;
        queue->tail = next;

        aeron_spsc_concurrent_linked_queue_node_delete(prev_tail);

        return element;
    }

    return NULL;
}
