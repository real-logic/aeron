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
#include "concurrent/aeron_broadcast_transmitter.h"
#include "concurrent/aeron_atomic.h"
#include "util/aeron_error.h"

int aeron_broadcast_transmitter_init(aeron_broadcast_transmitter_t *transmitter, void *buffer, size_t length)
{
    const size_t capacity = length - AERON_BROADCAST_BUFFER_TRAILER_LENGTH;
    int result = -1;

    if (AERON_BROADCAST_IS_CAPACITY_VALID(capacity))
    {
        transmitter->buffer = buffer;
        transmitter->capacity = capacity;
        transmitter->descriptor = (aeron_broadcast_descriptor_t *) (transmitter->buffer + transmitter->capacity);
        transmitter->max_message_length = AERON_BROADCAST_MAX_MESSAGE_LENGTH(transmitter->capacity);
        result = 0;
    }
    else
    {
        AERON_SET_ERR(EINVAL, "Capacity: %" PRIu64 " invalid, must be power of two", (uint64_t)capacity);
    }

    return result;
}

inline static void signal_tail_intent(aeron_broadcast_descriptor_t *descriptor, int64_t new_tail)
{
    AERON_SET_RELEASE(descriptor->tail_intent_counter, new_tail);
    aeron_release();  /* storeFence */
}

inline static void insert_padding_record(aeron_broadcast_record_descriptor_t *record, int32_t length)
{
    record->msg_type_id = AERON_BROADCAST_PADDING_MSG_TYPE_ID;
    record->length = length;
}

int aeron_broadcast_transmitter_transmit(
    aeron_broadcast_transmitter_t *transmitter, int32_t msg_type_id, const void *msg, size_t length)
{
    if (length > transmitter->max_message_length || AERON_BROADCAST_INVALID_MSG_TYPE_ID(msg_type_id))
    {
        AERON_SET_ERR(
            EINVAL,
            "length (%" PRIu64 ") > transmitter->max_message_length (%" PRIu64 ") || msg_type_id (%" PRId32 ") < 1",
            (uint64_t)length,
            (uint64_t)transmitter->max_message_length,
            msg_type_id);
        return -1;
    }

    int64_t current_tail = transmitter->descriptor->tail_counter;
    size_t record_offset = (uint32_t)current_tail & (transmitter->capacity - 1);
    const size_t record_length = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    const size_t aligned_record_length = AERON_ALIGN(record_length, AERON_BROADCAST_RECORD_ALIGNMENT);
    const int64_t new_tail = current_tail + (int64_t)aligned_record_length;

    const size_t to_end_of_buffer = (uint32_t)transmitter->capacity - record_offset;

    if (to_end_of_buffer < aligned_record_length)
    {
        signal_tail_intent(transmitter->descriptor, new_tail + (int64_t)to_end_of_buffer);
        insert_padding_record(
            (aeron_broadcast_record_descriptor_t *)(transmitter->buffer + record_offset), (int32_t)to_end_of_buffer);

        current_tail += (int64_t)to_end_of_buffer;
        record_offset = 0;
    }
    else
    {
        signal_tail_intent(transmitter->descriptor, new_tail);
    }

    aeron_broadcast_record_descriptor_t *record =
        (aeron_broadcast_record_descriptor_t *)(transmitter->buffer + record_offset);

    record->length = (int32_t)record_length;
    record->msg_type_id = msg_type_id;

    memcpy(transmitter->buffer + record_offset + AERON_BROADCAST_RECORD_HEADER_LENGTH, msg, length);

    AERON_SET_RELEASE(transmitter->descriptor->latest_counter, current_tail);
    AERON_SET_RELEASE(transmitter->descriptor->tail_counter, current_tail + aligned_record_length);

    return 0;
}
