/*
 * Copyright 2014-2020 Real Logic Limited.
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

#include <string.h>
#include <errno.h>
#include "concurrent/aeron_broadcast_receiver.h"
#include "aeron_atomic.h"
#include "util/aeron_error.h"

int aeron_broadcast_receiver_init(volatile aeron_broadcast_receiver_t *receiver, void *buffer, size_t length)
{
    const size_t capacity = length - AERON_BROADCAST_BUFFER_TRAILER_LENGTH;
    int result = -1;

    if (AERON_BROADCAST_IS_CAPACITY_VALID(capacity))
    {
        receiver->buffer = buffer;
        receiver->capacity = capacity;
        receiver->mask = capacity - 1;
        receiver->descriptor = (aeron_broadcast_descriptor_t *) (receiver->buffer + receiver->capacity);

        receiver->record_offset = 0;
        receiver->cursor = 0;
        receiver->next_record = 0;
        receiver->lapped_count = 0;

        result = 0;
    }
    else
    {
        aeron_set_err(EINVAL, "%s:%d: %s", __FILE__, __LINE__, strerror(EINVAL));
    }

    return result;
}

inline static bool aeron_broadcast_receiver_validate_at(volatile aeron_broadcast_receiver_t *receiver, int64_t cursor)
{
    int64_t tail_intent_counter;
    AERON_GET_VOLATILE(tail_intent_counter, receiver->descriptor->tail_intent_counter);

    return (cursor + (int64_t)receiver->capacity) > tail_intent_counter;
}

inline static bool aeron_broadcast_receiver_validate(volatile aeron_broadcast_receiver_t *receiver)
{
    aeron_acquire();

    return aeron_broadcast_receiver_validate_at(receiver, receiver->cursor);
}

inline static bool aeron_broadcast_receiver_receive_next(volatile aeron_broadcast_receiver_t *receiver)
{
    bool is_available = false;
    int64_t tail;
    int64_t cursor = receiver->next_record;

    AERON_GET_VOLATILE(tail, receiver->descriptor->tail_counter);

    if (tail > cursor)
    {
        size_t record_offset = (uint32_t)cursor & (receiver->capacity - 1);

        if (!aeron_broadcast_receiver_validate_at(receiver, cursor))
        {
            receiver->lapped_count++;
            cursor = receiver->descriptor->latest_counter;
            record_offset = (uint32_t)cursor & (receiver->capacity - 1);
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
    volatile aeron_broadcast_receiver_t *receiver, aeron_broadcast_receiver_handler_t handler)
{
    int messages_received = 0;
    const long last_seen_lapped_count = receiver->lapped_count;

    if (aeron_broadcast_receiver_receive_next(receiver))
    {
        if (last_seen_lapped_count != receiver->lapped_count)
        {
            aeron_set_err(EINVAL, "unable to keep up with broadcast");
            return -1;
        }

        aeron_broadcast_record_descriptor_t *record =
            (aeron_broadcast_record_descriptor_t *)(receiver->buffer + receiver->record_offset);

        const size_t length = (size_t)record->length - AERON_BROADCAST_RECORD_HEADER_LENGTH;

        if (length > sizeof(receiver->scratch_buffer))
        {
            aeron_set_err(EINVAL, "scratch buffer too small");
            return -1;
        }

        const int32_t type_id = record->msg_type_id;
        memcpy(
            (void *)receiver->scratch_buffer,
            receiver->buffer + receiver->record_offset + AERON_BROADCAST_RECORD_HEADER_LENGTH,
            length);

        if (!aeron_broadcast_receiver_validate(receiver))
        {
            aeron_set_err(EINVAL, "unable to keep up with broadcast");
            return -1;
        }

        handler(type_id, (uint8_t *)receiver->scratch_buffer, 0, (int)length);

        messages_received = 1;
    }

    return messages_received;
}
