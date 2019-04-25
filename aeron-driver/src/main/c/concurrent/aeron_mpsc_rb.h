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

#ifndef AERON_MPSC_RB_H
#define AERON_MPSC_RB_H

#include <concurrent/aeron_rb.h>

typedef struct aeron_mpsc_rb_stct
{
    uint8_t *buffer;
    aeron_rb_descriptor_t *descriptor;
    size_t capacity;
    size_t max_message_length;
}
aeron_mpsc_rb_t;

int aeron_mpsc_rb_init(volatile aeron_mpsc_rb_t *ring_buffer, void *buffer, size_t length);

aeron_rb_write_result_t aeron_mpsc_rb_write(
    volatile aeron_mpsc_rb_t *ring_buffer,
    int32_t msg_type_id,
    const void *msg,
    size_t length);

size_t aeron_mpsc_rb_read(
    volatile aeron_mpsc_rb_t *ring_buffer,
    aeron_rb_handler_t handler,
    void *clientd,
    size_t message_count_limit);

int64_t aeron_mpsc_rb_next_correlation_id(volatile aeron_mpsc_rb_t *ring_buffer);

void aeron_mpsc_rb_consumer_heartbeat_time(volatile aeron_mpsc_rb_t *ring_buffer, int64_t now_ms);
int64_t aeron_mpsc_rb_consumer_heartbeat_time_value(volatile aeron_mpsc_rb_t *ring_buffer);

bool aeron_mpsc_rb_unblock(volatile aeron_mpsc_rb_t *ring_buffer);

inline int64_t aeron_mpsc_rb_consumer_position(volatile aeron_mpsc_rb_t *ring_buffer)
{
    int64_t position;
    AERON_GET_VOLATILE(position, ring_buffer->descriptor->head_position);
    return position;
}

inline int64_t aeron_mpsc_rb_producer_position(volatile aeron_mpsc_rb_t *ring_buffer)
{
    int64_t position;
    AERON_GET_VOLATILE(position, ring_buffer->descriptor->tail_position);
    return position;
}

#endif //AERON_MPSC_RB_H
