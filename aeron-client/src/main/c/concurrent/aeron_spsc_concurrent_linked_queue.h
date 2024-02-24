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
#ifndef AERON_SPSC_CONCURRENT_LINKED_QUEUE_H
#define AERON_SPSC_CONCURRENT_LINKED_QUEUE_H

typedef struct aeron_spsc_concurrent_linked_queue_node_stct aeron_spsc_concurrent_linked_queue_node_t;

typedef struct aeron_spsc_concurrent_linked_queue_stct
{
    aeron_spsc_concurrent_linked_queue_node_t *head;
    aeron_spsc_concurrent_linked_queue_node_t *tail;
}
aeron_spsc_concurrent_linked_queue_t;

int aeron_spsc_concurrent_linked_queue_init(aeron_spsc_concurrent_linked_queue_t *queue);

int aeron_spsc_concurrent_linked_queue_close(aeron_spsc_concurrent_linked_queue_t *queue);

int aeron_spsc_concurrent_linked_queue_offer(aeron_spsc_concurrent_linked_queue_t *queue, void *element);

void *aeron_spsc_concurrent_linked_queue_poll(aeron_spsc_concurrent_linked_queue_t *queue);

#endif //AERON_SPSC_CONCURRENT_LINKED_QUEUE_H
