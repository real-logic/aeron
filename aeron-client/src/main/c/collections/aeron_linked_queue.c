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
#include "aeron_alloc.h"
#include "collections/aeron_linked_queue.h"
#include "util/aeron_error.h"

struct aeron_linked_queue_node_stct {
    aeron_linked_queue_node_t *next;
    void *element;
};

int aeron_linked_queue_node_create(void *element, aeron_linked_queue_node_t **nodep)
{
    aeron_linked_queue_node_t *node;

    if (aeron_alloc((void **)&node, sizeof(aeron_linked_queue_node_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    node->next = NULL;
    node->element = element;

    *nodep = node;

    return 0;
}

int aeron_linked_queue_init(aeron_linked_queue_t *queue)
{
    aeron_linked_queue_node_t *node;

    if (aeron_linked_queue_node_create(NULL, &node))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    queue->head = node;
    queue->tail = node;

    return 0;
}

int aeron_linked_queue_close(aeron_linked_queue_t *queue)
{
    if (NULL == queue->tail->next)
    {
        // the queue is empty
        aeron_linked_queue_node_delete(queue->tail);

        return 0;
    }

    // the queue is NOT empty
    AERON_SET_ERR(EINVAL, "%s", "queue must be empty to be deleted");
    return -1;
}

int aeron_linked_queue_offer(aeron_linked_queue_t *queue, void *element)
{
    aeron_linked_queue_node_t *node, *prev;

    if (aeron_linked_queue_node_create(element, &node))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    prev = queue->head;
    queue->head = node;
    prev->next = node;

    return 0;
}

void *aeron_linked_queue_peek(aeron_linked_queue_t *queue)
{
    return queue->tail->next == NULL ? NULL : queue->tail->next->element;
}

void *aeron_linked_queue_poll(aeron_linked_queue_t *queue)
{
    return aeron_linked_queue_poll_ex(queue, NULL);
}

void *aeron_linked_queue_poll_ex(aeron_linked_queue_t *queue, aeron_linked_queue_node_t **out_nodep)
{
    aeron_linked_queue_node_t *next;

    next = queue->tail->next;

    if (NULL != next)
    {
        aeron_linked_queue_node_t *prev_tail;
        void *element;

        prev_tail = queue->tail;
        element = next->element;
        queue->tail = next;

        if (NULL == out_nodep)
        {
            aeron_linked_queue_node_delete(prev_tail);
        }
        else
        {
            *out_nodep = prev_tail;
        }

        return element;
    }

    return NULL;
}

int aeron_linked_queue_node_delete(aeron_linked_queue_node_t *node)
{
    if (NULL != node)
    {
        aeron_free(node);
    }

    return 0;
}
