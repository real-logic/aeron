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

#ifndef AERON_C_TERM_APPENDER_H
#define AERON_C_TERM_APPENDER_H

#include <errno.h>
#include <inttypes.h>

#include "aeronc.h"
#include "util/aeron_fileutil.h"
#include "concurrent/aeron_atomic.h"
#include "util/aeron_error.h"

#define AERON_TERM_APPENDER_FAILED (-2)

inline int64_t aeron_term_appender_get_and_add_raw_tail(volatile int64_t *addr, size_t aligned_length)
{
    int64_t result = 0;

    AERON_GET_AND_ADD_INT64(result, *addr, aligned_length);
    return result;
}

inline int aeron_term_appender_check_term(int32_t expected_term_id, int32_t term_id)
{
    if (expected_term_id != term_id)
    {
        aeron_set_err(EINVAL, "action possibly delayed: expectedTermId=%" PRId32 " termId=%" PRId32,
            expected_term_id, term_id);
        return -1;
    }

    return 0;
}

inline void aeron_term_appender_header_write(
    aeron_mapped_buffer_t *term_buffer,
    int32_t offset,
    size_t length,
    int32_t term_id,
    int32_t session_id,
    int32_t stream_id)
{
    aeron_data_header_t *header = (aeron_data_header_t *)(term_buffer->addr + offset);

    AERON_PUT_ORDERED(header->frame_header.frame_length, (-length));
    aeron_release();

    header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    header->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG;
    header->frame_header.type = AERON_HDR_TYPE_DATA;
    header->term_offset = offset;
    header->session_id = session_id;
    header->stream_id = stream_id;
    header->term_id = term_id;
}

inline int32_t aeron_term_appender_handle_end_of_log_condition(
    aeron_mapped_buffer_t *term_buffer,
    int32_t term_offset,
    int32_t term_length,
    int32_t term_id,
    int32_t session_id,
    int32_t stream_id)
{
    if (term_offset < term_length)
    {
        const int32_t padding_length = term_length - term_offset;
        aeron_data_header_t *header = (aeron_data_header_t *)(term_buffer->addr + term_offset);

        aeron_term_appender_header_write(term_buffer, term_offset, padding_length, term_id, session_id, stream_id);
        header->frame_header.type = AERON_HDR_TYPE_PAD;
        AERON_PUT_ORDERED(header->frame_header.frame_length, padding_length);
    }

    return AERON_TERM_APPENDER_FAILED;
}

inline int32_t aeron_term_appender_claim(
    aeron_mapped_buffer_t *term_buffer,
    volatile int64_t *term_tail_counter,
    size_t length,
    aeron_buffer_claim_t *buffer_claim,
    int32_t active_term_id,
    int32_t session_id,
    int32_t stream_id)
{
    const size_t frame_length = length + AERON_DATA_HEADER_LENGTH;
    const int64_t aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t raw_tail = aeron_term_appender_get_and_add_raw_tail(term_tail_counter, aligned_frame_length);
    const int64_t term_offset = raw_tail & 0xFFFFFFFF;
    const int32_t term_id = aeron_logbuffer_term_id(raw_tail);
    const int32_t term_length = term_buffer->length;

    if (aeron_term_appender_check_term(active_term_id, term_id) < 0)
    {
        return -1;
    }

    int64_t resulting_offset = term_offset + aligned_frame_length;
    if (resulting_offset > term_length)
    {
        resulting_offset = aeron_term_appender_handle_end_of_log_condition(
            term_buffer, term_offset, term_length, term_id, session_id, stream_id);
    }
    else
    {
        aeron_term_appender_header_write(term_buffer, term_offset, frame_length, term_id, session_id, stream_id);
        buffer_claim->frame_header = term_buffer->addr + term_offset;
        buffer_claim->data = buffer_claim->frame_header + AERON_DATA_HEADER_LENGTH;
        buffer_claim->length = length;
    }

    return resulting_offset;
}

inline int32_t aeron_term_appender_append_unfragmented_message(
    aeron_mapped_buffer_t *term_buffer,
    volatile int64_t *term_tail_counter,
    uint8_t *buffer,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd,
    int32_t active_term_id,
    int32_t session_id,
    int32_t stream_id)
{
    const size_t frame_length = length + AERON_DATA_HEADER_LENGTH;
    const int64_t aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t raw_tail = aeron_term_appender_get_and_add_raw_tail(term_tail_counter, aligned_frame_length);
    const int64_t term_offset = raw_tail & 0xFFFFFFFF;
    const int32_t term_id = aeron_logbuffer_term_id(raw_tail);
    const int32_t term_length = term_buffer->length;

    if (aeron_term_appender_check_term(active_term_id, term_id) < 0)
    {
        return -1;
    }

    int64_t resulting_offset = term_offset + aligned_frame_length;
    if (resulting_offset > term_length)
    {
        resulting_offset = aeron_term_appender_handle_end_of_log_condition(
            term_buffer, term_offset, term_length, term_id, session_id, stream_id);
    }
    else
    {
        aeron_term_appender_header_write(term_buffer, term_offset, frame_length, term_id, session_id, stream_id);
        memcpy(term_buffer->addr + term_offset + AERON_DATA_HEADER_LENGTH, buffer, length);

        aeron_data_header_t *data_header = (aeron_data_header_t *)(term_buffer->addr + term_offset);

        data_header->reserved_value = reserved_value_supplier(clientd, term_buffer->addr + term_offset, frame_length);
        AERON_PUT_ORDERED(data_header->frame_header.frame_length, frame_length);
    }

    return resulting_offset;
}

inline int32_t aeron_term_appender_append_unfragmented_messagev(
    aeron_mapped_buffer_t *term_buffer,
    volatile int64_t *term_tail_counter,
    aeron_iovec_t *iov,
    size_t iovcnt,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd,
    int32_t active_term_id,
    int32_t session_id,
    int32_t stream_id)
{
    const size_t frame_length = length + AERON_DATA_HEADER_LENGTH;
    const int64_t aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t raw_tail = aeron_term_appender_get_and_add_raw_tail(term_tail_counter, aligned_frame_length);
    const int64_t term_offset = raw_tail & 0xFFFFFFFF;
    const int32_t term_id = aeron_logbuffer_term_id(raw_tail);
    const int32_t term_length = term_buffer->length;

    if (aeron_term_appender_check_term(active_term_id, term_id) < 0)
    {
        return -1;
    }

    int64_t resulting_offset = term_offset + aligned_frame_length;
    if (resulting_offset > term_length)
    {
        resulting_offset = aeron_term_appender_handle_end_of_log_condition(
        term_buffer, term_offset, term_length, term_id, session_id, stream_id);
    }
    else
    {
        aeron_term_appender_header_write(term_buffer, term_offset, frame_length, term_id, session_id, stream_id);

        aeron_data_header_t *data_header = (aeron_data_header_t *)(term_buffer->addr + term_offset);
        int32_t offset = term_offset + AERON_DATA_HEADER_LENGTH;
        size_t i = 0;

        for (int32_t ending_offset = offset + length; offset < ending_offset; offset += iov[i].iov_len, i++)
        {
            memcpy(term_buffer->addr + offset, iov[i].iov_base, iov[i].iov_len);
        }

        data_header->reserved_value = reserved_value_supplier(clientd, term_buffer->addr + term_offset, frame_length);
        AERON_PUT_ORDERED(data_header->frame_header.frame_length, frame_length);
    }

    return resulting_offset;
}

#endif //AERON_C_TERM_APPENDER_H
