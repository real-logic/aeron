/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

#ifndef AERON_AERON_LOGBUFFER_DESCRIPTOR_H
#define AERON_AERON_LOGBUFFER_DESCRIPTOR_H

#include <assert.h>
#include <protocol/aeron_udp_protocol.h>
#include "util/aeron_bitutil.h"
#include "concurrent/aeron_atomic.h"

#define AERON_LOGBUFFER_PARTITION_COUNT (3)
#define AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH  (AERON_CACHE_LINE_LENGTH * 2)

#define AERON_MAX_UDP_PAYLOAD_LENGTH (65504)

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_logbuffer_metadata_stct
{
    int64_t term_tail_counters[AERON_LOGBUFFER_PARTITION_COUNT];
    int32_t active_partition_index;
    uint8_t pad1[(2 * AERON_CACHE_LINE_LENGTH) - ((AERON_LOGBUFFER_PARTITION_COUNT * sizeof(int64_t)) + sizeof(int32_t))];
    int64_t time_of_last_status_message;
    int64_t end_of_stream_position;
    uint8_t pad2[(2 * AERON_CACHE_LINE_LENGTH) - (2 * sizeof(int64_t))];
    int64_t correlation_id;
    int32_t initialTerm_id;
    int32_t default_frame_header_length;
    int32_t mtu_length;
    uint8_t pad3[(AERON_CACHE_LINE_LENGTH) - (5 * sizeof(int32_t))];
}
aeron_logbuffer_metadata_t;
#pragma pack(pop)

#define AERON_LOGBUFFER_META_DATA_LENGTH (sizeof(aeron_logbuffer_metadata_t) + AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH)

#define AERON_LOGBUFFER_COMPUTE_LOG_LENGTH(term_length) ((term_length * AERON_LOGBUFFER_PARTITION_COUNT) + AERON_LOGBUFFER_META_DATA_LENGTH)

#define AERON_LOGBUFFER_ACTIVE_PARTITION_INDEX_VOLATILE(d,m) (AERON_GET_VOLATILE(d,(m->active_partition_index)))

#define AERON_LOGBUFFER_RAWTAIL_VOLATILE(d,m) \
do \
{ \
    size_t partition; \
    AERON_GET_VOLATILE(partition,(m->active_partition_index)); \
    AERON_GET_VOLATILE(d, m->term_tail_counters[partition]); \
} \
while(0)

inline int32_t aeron_logbuffer_term_offset(int64_t raw_tail, int32_t term_length)
{
    int32_t offset = (int32_t)(raw_tail & 0xFFFFFFFFL);

    return (offset < term_length) ? offset : term_length;
}

inline int32_t aeron_logbuffer_term_id(int64_t raw_tail)
{
    return (int32_t)(raw_tail >> 32);
}

inline size_t aeron_logbuffer_index_by_position(int64_t position, size_t position_bits_to_shift)
{
    return (size_t)((position >> position_bits_to_shift) % AERON_LOGBUFFER_PARTITION_COUNT);
}

inline int64_t aeron_logbuffer_compute_position(
    int32_t active_term_id, int32_t term_offset, size_t position_bits_to_shift, int32_t initial_term_id)
{
    int32_t term_count = active_term_id - initial_term_id;

    return (term_count << position_bits_to_shift) + term_offset;
}

inline void aeron_logbuffer_fill_default_header(
    uint8_t *log_meta_data_buffer, int32_t session_id, int32_t stream_id, int32_t initial_term_id)
{
    aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)log_meta_data_buffer;
    aeron_data_header_t *data_header = (aeron_data_header_t *)(log_meta_data_buffer + sizeof(aeron_logbuffer_metadata_t));

    log_meta_data->default_frame_header_length = AERON_DATA_HEADER_LENGTH;
    data_header->frame_header.frame_length = 0;
    data_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    data_header->frame_header.flags = (int8_t)(AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG);
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;
    data_header->stream_id = stream_id;
    data_header->session_id = session_id;
    data_header->term_id = initial_term_id;
    data_header->term_offset = 0;
    data_header->reserved_value = AERON_DATA_HEADER_DEFAULT_RESERVED_VALUE;
}

#endif //AERON_AERON_LOGBUFFER_DESCRIPTOR_H
