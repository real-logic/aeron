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
#ifndef AERON_LINKED_QUEUE_H
#define AERON_LINKED_QUEUE_H

typedef struct aeron_linked_queue_node_stct aeron_linked_queue_node_t;

typedef struct aeron_linked_queue_stct
{
    aeron_linked_queue_node_t *head;
    aeron_linked_queue_node_t *tail;
}
aeron_linked_queue_t;


int aeron_linked_queue_init(aeron_linked_queue_t *queue);

int aeron_linked_queue_close(aeron_linked_queue_t *queue);

int aeron_linked_queue_offer(aeron_linked_queue_t *queue, void *element);

int aeron_linked_queue_offer_ex(aeron_linked_queue_t *queue, void *element, aeron_linked_queue_node_t *node);

void *aeron_linked_queue_peek(aeron_linked_queue_t *queue);

void *aeron_linked_queue_poll(aeron_linked_queue_t *queue);

// set out_nodep to retrieve the underlying aeron_linked_queue_node_t for subsequent deletion
void *aeron_linked_queue_poll_ex(aeron_linked_queue_t *queue, aeron_linked_queue_node_t **out_nodep);

int aeron_linked_queue_node_delete(aeron_linked_queue_node_t *node);

#endif //AERON_LINKED_QUEUE_H
