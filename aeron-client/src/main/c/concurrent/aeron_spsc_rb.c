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

#include "aeron_spsc_rb.h"

int aeron_spsc_rb_init(volatile aeron_spsc_rb_t *ring_buffer, void *buffer, size_t length)
{
    const size_t capacity = length - AERON_RB_TRAILER_LENGTH;
    int result = -1;

    if (AERON_RB_IS_CAPACITY_VALID(capacity))
    {
        ring_buffer->buffer = buffer;
        ring_buffer->capacity = capacity;
        ring_buffer->descriptor = (aeron_rb_descriptor_t *)(ring_buffer->buffer + ring_buffer->capacity);
        ring_buffer->max_message_length = AERON_RB_MAX_MESSAGE_LENGTH(ring_buffer->capacity);
        result = 0;
    }

    return result;
}

aeron_rb_write_result_t aeron_spsc_rb_write(
    volatile aeron_spsc_rb_t *ring_buffer,
    int32_t msg_type_id,
    const void *msg,
    size_t length)
{
    struct iovec vec[1];
    vec[0].iov_len = length;
    vec[0].iov_base = (void*) msg;

    return aeron_spsc_rb_writev(ring_buffer, msg_type_id, vec, 1);
}

aeron_rb_write_result_t aeron_spsc_rb_writev(
    volatile aeron_spsc_rb_t *ring_buffer,
    int32_t msg_type_id,
    const struct iovec* iov,
    int iovcnt)
{
    size_t length = 0;
    for (int i = 0; i < iovcnt; i++)
    {
        length += iov[i].iov_len;
    }

    const size_t record_length = length + AERON_RB_RECORD_HEADER_LENGTH;
    const size_t aligned_record_length = AERON_ALIGN(record_length, AERON_RB_ALIGNMENT);
    const size_t required_capacity = aligned_record_length + AERON_RB_RECORD_HEADER_LENGTH;
    const size_t mask = ring_buffer->capacity - 1;

    int64_t head = ring_buffer->descriptor->head_cache_position;
    int64_t tail = ring_buffer->descriptor->tail_position;
    const int32_t available_capacity = (int32_t)ring_buffer->capacity - (int32_t)(tail - head);

    size_t padding = 0;
    size_t record_index = (size_t)tail & mask;
    const size_t to_buffer_end_length = ring_buffer->capacity - record_index;
    aeron_rb_record_descriptor_t *record_header = NULL, *next_header = NULL;

    if (length > ring_buffer->max_message_length || AERON_RB_INVALID_MSG_TYPE_ID(msg_type_id))
    {
        return AERON_RB_ERROR;
    }

    if ((int32_t)required_capacity > available_capacity)
    {
        AERON_GET_VOLATILE(head, ring_buffer->descriptor->head_position);

        if (required_capacity > (ring_buffer->capacity - (size_t)(tail - head)))
        {
            return AERON_RB_FULL;
        }

        ring_buffer->descriptor->head_cache_position = head;
    }

    if (required_capacity > to_buffer_end_length)
    {
        size_t head_index = (int32_t)(head & mask);

        if (required_capacity > head_index)
        {
            AERON_GET_VOLATILE(head, ring_buffer->descriptor->head_position);
            head_index = (int32_t)(head & mask);

            if (required_capacity > head_index)
            {
                return AERON_RB_FULL;
            }

            AERON_PUT_ORDERED(ring_buffer->descriptor->head_cache_position, head);
        }

        padding = to_buffer_end_length;
    }

    if (0 != padding)
    {
        record_header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
        next_header = (aeron_rb_record_descriptor_t *)ring_buffer->buffer;

        next_header->length = 0;
        record_header->msg_type_id = AERON_RB_PADDING_MSG_TYPE_ID;
        AERON_PUT_ORDERED(record_header->length, (int32_t)padding);
        record_index = 0;
    }

    record_header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
    next_header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index + aligned_record_length);

    size_t current_vector_offset = 0;
    for (int i = 0; i < iovcnt; i++)
    {
        uint8_t* offset = ring_buffer->buffer + AERON_RB_MESSAGE_OFFSET(record_index) + current_vector_offset;
        memcpy(offset, iov[i].iov_base, iov[i].iov_len);
        current_vector_offset += iov[i].iov_len;
    }

    next_header->length = 0;
    record_header->msg_type_id = msg_type_id;
    AERON_PUT_ORDERED(record_header->length, (int32_t)record_length);
    AERON_PUT_ORDERED(ring_buffer->descriptor->tail_position, tail + aligned_record_length + padding);

    return AERON_RB_SUCCESS;
}


size_t aeron_spsc_rb_read(
    volatile aeron_spsc_rb_t *ring_buffer,
    aeron_rb_handler_t handler,
    void *clientd,
    size_t message_count_limit)
{
    const int64_t head = ring_buffer->descriptor->head_position;
    const size_t head_index = (int32_t)(head & (ring_buffer->capacity - 1));
    const size_t contiguous_block_length = ring_buffer->capacity - head_index;
    size_t messages_read = 0;
    size_t bytes_read = 0;

    while ((bytes_read < contiguous_block_length) && (messages_read < message_count_limit))
    {
        aeron_rb_record_descriptor_t *header = NULL;
        const size_t record_index = head_index + bytes_read;
        int32_t record_length = 0;

        header = (aeron_rb_record_descriptor_t *)(ring_buffer->buffer + record_index);
        AERON_GET_VOLATILE(record_length, header->length);

        if (record_length <= 0)
        {
            break;
        }

        bytes_read += AERON_ALIGN(record_length, AERON_RB_ALIGNMENT);
        int32_t msg_type_id = header->msg_type_id;

        if (AERON_RB_PADDING_MSG_TYPE_ID == msg_type_id)
        {
            continue;
        }

        ++messages_read;
        handler(
            msg_type_id,
            ring_buffer->buffer + AERON_RB_MESSAGE_OFFSET(record_index),
            record_length - AERON_RB_RECORD_HEADER_LENGTH,
            clientd);
    }

    if (0 != bytes_read)
    {
        AERON_PUT_ORDERED(ring_buffer->descriptor->head_position, head + bytes_read);
    }

    return messages_read;
}

int64_t aeron_spsc_rb_next_correlation_id(volatile aeron_spsc_rb_t *ring_buffer)
{
    return aeron_get_and_add_int64(&(ring_buffer->descriptor->correlation_counter), 1);
}

void aeron_spsc_rb_consumer_heartbeat_time(volatile aeron_spsc_rb_t *ring_buffer, int64_t time_ms)
{
    AERON_PUT_ORDERED(ring_buffer->descriptor->consumer_heartbeat, time_ms);
}
