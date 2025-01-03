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

#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include "aeron_alloc.h"
#include "concurrent/aeron_broadcast_receiver.h"
#include "util/aeron_error.h"

int aeron_broadcast_receiver_init(aeron_broadcast_receiver_t *receiver, void *buffer, size_t length)
{
    const size_t capacity = length - AERON_BROADCAST_BUFFER_TRAILER_LENGTH;
    receiver->scratch_buffer = NULL;
    receiver->scratch_buffer_capacity = 0;

    if (AERON_BROADCAST_IS_CAPACITY_VALID(capacity))
    {
        size_t scratch_buffer_capacity = AERON_BROADCAST_SCRATCH_BUFFER_LENGTH_DEFAULT;
        uint8_t *scratch_buffer;
        if (aeron_alloc((void**)&scratch_buffer, scratch_buffer_capacity) < 0)
        {
            AERON_APPEND_ERR("failed to allocate scratch buffer of capacity: %" PRIu64, scratch_buffer_capacity);
            return -1;
        }

        receiver->scratch_buffer = scratch_buffer;
        receiver->scratch_buffer_capacity = scratch_buffer_capacity;
        receiver->buffer = buffer;
        receiver->capacity = capacity;
        receiver->mask = capacity - 1u;
        receiver->descriptor = (aeron_broadcast_descriptor_t *)(receiver->buffer + receiver->capacity);

        int64_t latest;
        AERON_GET_ACQUIRE(latest, receiver->descriptor->latest_counter);

        receiver->cursor = latest;
        receiver->next_record = latest;
        receiver->record_offset = (size_t)latest & receiver->mask;
        receiver->lapped_count = 0;

        return 0;
    }
    else
    {
        AERON_SET_ERR(EINVAL, "Capacity: %" PRIu64 " invalid, must be power of two", (uint64_t)capacity);
        return -1;
    }
}

int aeron_broadcast_receiver_close(aeron_broadcast_receiver_t *receiver)
{
    aeron_free(receiver->scratch_buffer);
    receiver->scratch_buffer = NULL;
    receiver->scratch_buffer_capacity = 0;
    return 0;
}

int aeron_broadcast_receiver_receive(
    aeron_broadcast_receiver_t *receiver, aeron_broadcast_receiver_handler_t handler, void *clientd)
{
    int messages_received = 0;
    const long last_seen_lapped_count = receiver->lapped_count;

    if (aeron_broadcast_receiver_receive_next(receiver))
    {
        if (last_seen_lapped_count != receiver->lapped_count)
        {
            AERON_SET_ERR(EINVAL, "%s", "unable to keep up with broadcast");
            return -1;
        }

        aeron_broadcast_record_descriptor_t *record =
            (aeron_broadcast_record_descriptor_t *)(receiver->buffer + receiver->record_offset);

        const size_t length = (size_t)record->length - AERON_BROADCAST_RECORD_HEADER_LENGTH;

        if (length > receiver->scratch_buffer_capacity)
        {
            size_t new_scratch_buffer_capacity = receiver->scratch_buffer_capacity;
            while (new_scratch_buffer_capacity < length)
            {
                new_scratch_buffer_capacity += (new_scratch_buffer_capacity >> 1);
            }

            uint8_t *new_scratch_buffer;
            if (aeron_alloc((void**)&new_scratch_buffer, new_scratch_buffer_capacity) < 0)
            {
                AERON_APPEND_ERR("failed to allocate scratch buffer of capacity: %" PRIu64, new_scratch_buffer_capacity);
                return -1;
            }

            aeron_free(receiver->scratch_buffer);
            receiver->scratch_buffer = new_scratch_buffer;
            receiver->scratch_buffer_capacity = new_scratch_buffer_capacity;
        }

        const int32_t type_id = record->msg_type_id;
        memcpy(
            (void *)receiver->scratch_buffer,
            receiver->buffer + receiver->record_offset + AERON_BROADCAST_RECORD_HEADER_LENGTH,
            length);

        if (!aeron_broadcast_receiver_validate(receiver))
        {
            AERON_SET_ERR(EINVAL, "%s", "unable to keep up with broadcast");
            return -1;
        }

        handler(type_id, (uint8_t *)receiver->scratch_buffer, length, clientd);

        messages_received = 1;
    }

    return messages_received;
}

extern bool aeron_broadcast_receiver_validate(aeron_broadcast_receiver_t *receiver);

extern bool aeron_broadcast_receiver_validate_at(aeron_broadcast_receiver_t *receiver, int64_t cursor);

extern bool aeron_broadcast_receiver_receive_next(aeron_broadcast_receiver_t *receiver);
