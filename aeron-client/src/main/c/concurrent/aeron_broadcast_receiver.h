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

#ifndef AERON_BROADCAST_RECEIVER_H
#define AERON_BROADCAST_RECEIVER_H

#include "util/aeron_bitutil.h"
#include "aeron_atomic.h"
#include "aeron_broadcast_descriptor.h"

#define AERON_BROADCAST_SCRATCH_BUFFER_LENGTH_DEFAULT (4096u)

typedef struct aeron_broadcast_receiver_stct
{
    uint8_t *scratch_buffer;
    uint8_t *buffer;
    aeron_broadcast_descriptor_t *descriptor;
    size_t capacity;
    size_t mask;
    size_t scratch_buffer_capacity;

    size_t record_offset;
    int64_t cursor;
    int64_t next_record;
    long lapped_count;
}
aeron_broadcast_receiver_t;

typedef void (*aeron_broadcast_receiver_handler_t)(int32_t type_id, uint8_t *buffer, size_t length, void *clientd);

int aeron_broadcast_receiver_init(aeron_broadcast_receiver_t *receiver, void *buffer, size_t length);
int aeron_broadcast_receiver_close(aeron_broadcast_receiver_t *receiver);

inline bool aeron_broadcast_receiver_validate_at(aeron_broadcast_receiver_t *receiver, int64_t cursor)
{
    int64_t tail_intent_counter;
    AERON_GET_ACQUIRE(tail_intent_counter, receiver->descriptor->tail_intent_counter);

    return (cursor + (int64_t)receiver->capacity) > tail_intent_counter;
}

inline bool aeron_broadcast_receiver_validate(aeron_broadcast_receiver_t *receiver)
{
    aeron_acquire();

    return aeron_broadcast_receiver_validate_at(receiver, receiver->cursor);
}

inline bool aeron_broadcast_receiver_receive_next(aeron_broadcast_receiver_t *receiver)
{
    bool is_available = false;
    int64_t tail;
    int64_t cursor = receiver->next_record;

    AERON_GET_ACQUIRE(tail, receiver->descriptor->tail_counter);

    if (tail > cursor)
    {
        size_t record_offset = (uint32_t)cursor & (receiver->capacity - 1u);

        if (!aeron_broadcast_receiver_validate_at(receiver, cursor))
        {
            receiver->lapped_count++;
            AERON_GET_ACQUIRE(cursor, receiver->descriptor->latest_counter);
            record_offset = (uint32_t)cursor & (receiver->capacity - 1u);
        }

        aeron_broadcast_record_descriptor_t *record =
            (aeron_broadcast_record_descriptor_t *)(receiver->buffer + record_offset);

        receiver->cursor = cursor;
        receiver->next_record = cursor + AERON_ALIGN(record->length, AERON_BROADCAST_RECORD_ALIGNMENT);

        if (AERON_BROADCAST_PADDING_MSG_TYPE_ID == record->msg_type_id)
        {
            aeron_broadcast_record_descriptor_t *new_record =
                (aeron_broadcast_record_descriptor_t *)(receiver->buffer + 0);

            record_offset = 0;
            receiver->cursor = receiver->next_record;
            receiver->next_record += AERON_ALIGN(new_record->length, AERON_BROADCAST_RECORD_ALIGNMENT);
        }

        receiver->record_offset = record_offset;
        is_available = true;
    }

    return is_available;
}

int aeron_broadcast_receiver_receive(
    aeron_broadcast_receiver_t *receiver, aeron_broadcast_receiver_handler_t handler, void *clientd);

#endif //AERON_BROADCAST_RECEIVER_H
