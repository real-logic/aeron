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

#ifndef AERON_LOGBUFFER_DESCRIPTOR_H
#define AERON_LOGBUFFER_DESCRIPTOR_H

#include <string.h>

#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_math.h"
#include "concurrent/aeron_atomic.h"

#define AERON_LOGBUFFER_PARTITION_COUNT (3)
#define AERON_LOGBUFFER_TERM_MIN_LENGTH (64 * 1024)
#define AERON_LOGBUFFER_TERM_MAX_LENGTH (1024 * 1024 * 1024)
#define AERON_PAGE_MIN_SIZE (4 * 1024)
#define AERON_PAGE_MAX_SIZE (1024 * 1024 * 1024)
#define AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH (AERON_CACHE_LINE_LENGTH * 2)

#define AERON_MAX_UDP_PAYLOAD_LENGTH (65504)

#ifdef _MSC_VER
#define _Static_assert static_assert
#endif

#ifdef __cplusplus
#define _Static_assert static_assert
#endif

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_logbuffer_metadata_stct
{
    volatile int64_t term_tail_counters[AERON_LOGBUFFER_PARTITION_COUNT];
    volatile int32_t active_term_count;
    uint8_t pad1[(2 * AERON_CACHE_LINE_LENGTH) - ((AERON_LOGBUFFER_PARTITION_COUNT * sizeof(int64_t)) + sizeof(int32_t))];
    volatile int64_t end_of_stream_position;
    volatile int32_t is_connected;
    volatile int32_t active_transport_count;
    uint8_t pad2[(2 * AERON_CACHE_LINE_LENGTH) - (sizeof(int64_t) + (2 * sizeof(int32_t)))];
    int64_t correlation_id;
    int32_t initial_term_id;
    int32_t default_frame_header_length;
    int32_t mtu_length;
    int32_t term_length;
    int32_t page_size;

    // new fields since Aeron 1.47.0
    int32_t socket_rcvbuf_length;
    int32_t socket_sndbuf_length;
    int32_t receiver_window_length;
    int32_t publication_window_length;
    int32_t max_resend;

     uint8_t sparse;
     uint8_t tether;
     uint8_t rejoin;
     uint8_t reliable;
     uint8_t signal_eos;
     uint8_t spies_simulate_connection;

     uint8_t pad3[2];
     int64_t linger_timeout_ns;

     uint8_t default_header[AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH];
//
//     int64_t untethered_window_limit_timeout_ns;
//     int64_t untethered_resting_timeout_ns;

//        int64_t entity_tag;
//        int64_t response_correlation_id;
//        uint8_t group;
//        uint8_t is_response;

    // todo: is this padding correct
    //uint8_t pad4[(AERON_CACHE_LINE_LENGTH) - (7 * sizeof(int32_t)) - (2 * sizeof(int64_t))];
    //uint8_t pad4[(AERON_CACHE_LINE_LENGTH) - (7 * sizeof(int32_t)) ];
}
aeron_logbuffer_metadata_t;
#pragma pack(pop)

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, term_tail_counters) == 0,
    "offsetof(aeron_logbuffer_metadata_t, term_tail_counters) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, active_term_count) == 24,
    "offsetof(aeron_logbuffer_metadata_t, active_term_count) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, end_of_stream_position) == 128,
    "offsetof(aeron_logbuffer_metadata_t, end_of_stream_position) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, is_connected) == 136,
    "offsetof(aeron_logbuffer_metadata_t, is_connected) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, active_transport_count) == 140,
    "offsetof(aeron_logbuffer_metadata_t, active_transport_count) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, correlation_id) == 256,
    "offsetof(aeron_logbuffer_metadata_t, correlation_id) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, initial_term_id) == 264,
    "offsetof(aeron_logbuffer_metadata_t, initial_term_id) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, default_frame_header_length) == 268,
    "offsetof(aeron_logbuffer_metadata_t, default_frame_header_length) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, mtu_length) == 272,
    "offsetof(aeron_logbuffer_metadata_t, mtu_length) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, term_length) == 276,
    "offsetof(aeron_logbuffer_metadata_t, term_length) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, page_size) == 280,
    "offsetof(aeron_logbuffer_metadata_t, page_size) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, socket_rcvbuf_length) == 284,
    "offsetof(aeron_logbuffer_metadata_t, socket_rcvbuf_length) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, socket_sndbuf_length) == 288,
    "offsetof(aeron_logbuffer_metadata_t, socket_sndbuf_length) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, receiver_window_length) == 292,
    "offsetof(aeron_logbuffer_metadata_t, receiver_window_length) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, publication_window_length) == 296,
    "offsetof(aeron_logbuffer_metadata_t, publication_window_length) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, max_resend) == 300,
    "offsetof(aeron_logbuffer_metadata_t, max_resend) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, sparse) == 304,
    "offsetof(aeron_logbuffer_metadata_t, sparse) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, tether) == 305,
    "offsetof(aeron_logbuffer_metadata_t, tether) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, rejoin) == 306,
    "offsetof(aeron_logbuffer_metadata_t, rejoin) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, reliable) == 307,
    "offsetof(aeron_logbuffer_metadata_t, reliable) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, signal_eos) == 308,
    "offsetof(aeron_logbuffer_metadata_t, signal_eos) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, spies_simulate_connection) == 309,
    "offsetof(aeron_logbuffer_metadata_t, spies_simulate_connection) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, linger_timeout_ns) == 312,
    "offsetof(aeron_logbuffer_metadata_t, linger_timeout_ns) is wrong");
//_Static_assert(
//    offsetof(aeron_logbuffer_metadata_t, default_header) == 320,
//    "offsetof(aeron_logbuffer_metadata_t, default_header) is wrong");
//
//_Static_assert(
//    AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH >= AERON_DATA_HEADER_LENGTH,
//    "AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH < AERON_DATA_HEADER_LENGTH");
//
//_Static_assert(
//    offsetof(aeron_logbuffer_metadata_t, untethered_window_limit_timeout_ns) == 448,
//    "offsetof(aeron_logbuffer_metadata_t, untethered_window_limit_timeout_ns) is wrong");
//_Static_assert(
//    offsetof(aeron_logbuffer_metadata_t, untethered_resting_timeout_ns) == 456,
//    "offsetof(aeron_logbuffer_metadata_t, untethered_resting_timeout_ns) is wrong");

////_Static_assert(
////    sizeof(aeron_logbuffer_metadata_t) == 480,
////    "sizeof(aeron_logbuffer_metadata_t) is wrong")



#define AERON_LOGBUFFER_META_DATA_LENGTH \
    (AERON_ALIGN((sizeof(aeron_logbuffer_metadata_t) + AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH), AERON_PAGE_MIN_SIZE))

#define AERON_LOGBUFFER_FRAME_ALIGNMENT (32)

#define AERON_LOGBUFFER_RAWTAIL_VOLATILE(d, m) \
do \
{ \
    int32_t active_term_count; \
    AERON_GET_ACQUIRE(active_term_count, ((m)->active_term_count)); \
    size_t partition = (size_t)(active_term_count % AERON_LOGBUFFER_PARTITION_COUNT); \
    AERON_GET_ACQUIRE(d, (m)->term_tail_counters[partition]); \
} \
while (false)

int aeron_logbuffer_check_term_length(uint64_t term_length);
int aeron_logbuffer_check_page_size(uint64_t page_size);

inline uint64_t aeron_logbuffer_compute_log_length(uint64_t term_length, uint64_t page_size)
{
    return AERON_ALIGN(((term_length * AERON_LOGBUFFER_PARTITION_COUNT) + AERON_LOGBUFFER_META_DATA_LENGTH), page_size);
}

inline int32_t aeron_logbuffer_term_offset(int64_t raw_tail, int32_t term_length)
{
    int64_t offset = raw_tail & 0xFFFFFFFFL;
    return offset < term_length ? (int32_t)offset : term_length;
}

inline int32_t aeron_logbuffer_term_id(int64_t raw_tail)
{
    return (int32_t)(raw_tail >> 32);
}

inline int32_t aeron_logbuffer_compute_term_count(int32_t term_id, int32_t initial_term_id)
{
    return aeron_sub_wrap_i32(term_id, initial_term_id);
}

inline size_t aeron_logbuffer_index_by_position(int64_t position, size_t position_bits_to_shift)
{
    return (size_t)((position >> position_bits_to_shift) % AERON_LOGBUFFER_PARTITION_COUNT);
}

inline size_t aeron_logbuffer_index_by_term(int32_t initial_term_id, int32_t active_term_id)
{
    int32_t term_count = aeron_logbuffer_compute_term_count(active_term_id, initial_term_id);
    return (size_t)(term_count % AERON_LOGBUFFER_PARTITION_COUNT);
}

inline size_t aeron_logbuffer_index_by_term_count(int32_t term_count)
{
    return (size_t)(term_count % AERON_LOGBUFFER_PARTITION_COUNT);
}

inline int64_t aeron_logbuffer_compute_position(
    int32_t active_term_id, int32_t term_offset, size_t position_bits_to_shift, int32_t initial_term_id)
{
    int64_t term_count = aeron_logbuffer_compute_term_count(active_term_id, initial_term_id);
    return (term_count << position_bits_to_shift) + term_offset;
}

inline int64_t aeron_logbuffer_compute_term_begin_position(
    int32_t active_term_id, size_t position_bits_to_shift, int32_t initial_term_id)
{
    return aeron_logbuffer_compute_position(active_term_id, 0, position_bits_to_shift, initial_term_id);
}

inline int32_t aeron_logbuffer_compute_term_id_from_position(
    int64_t position, size_t position_bits_to_shift, int32_t initial_term_id)
{
    return aeron_add_wrap_i32((int32_t)(position >> position_bits_to_shift), initial_term_id);
}

inline int32_t aeron_logbuffer_compute_term_offset_from_position(int64_t position, size_t position_bits_to_shift)
{
    int64_t mask = (1u << position_bits_to_shift) - 1;

    return (int32_t)(position & mask);
}

inline bool aeron_logbuffer_cas_raw_tail(
    aeron_logbuffer_metadata_t *log_meta_data,
    size_t partition_index,
    int64_t expected_raw_tail,
    int64_t update_raw_tail)
{
    return aeron_cas_int64(&log_meta_data->term_tail_counters[partition_index], expected_raw_tail, update_raw_tail);
}

inline int32_t aeron_logbuffer_active_term_count(aeron_logbuffer_metadata_t *log_meta_data)
{
    int32_t active_term_count;
    AERON_GET_ACQUIRE(active_term_count, log_meta_data->active_term_count);
    return active_term_count;
}

inline bool aeron_logbuffer_cas_active_term_count(
    aeron_logbuffer_metadata_t *log_meta_data,
    int32_t expected_term_count,
    int32_t update_term_count)
{
    return aeron_cas_int32(&log_meta_data->active_term_count, expected_term_count, update_term_count);
}

inline bool aeron_logbuffer_rotate_log(
    aeron_logbuffer_metadata_t *log_meta_data, int32_t current_term_count, int32_t current_term_id)
{
    const int32_t next_term_id = current_term_id + 1;
    const int32_t next_term_count = current_term_count + 1;
    const size_t next_index = aeron_logbuffer_index_by_term_count(next_term_count);
    const int32_t expected_term_id = next_term_id - AERON_LOGBUFFER_PARTITION_COUNT;

    int64_t raw_tail;
    do
    {
        AERON_GET_ACQUIRE(raw_tail, log_meta_data->term_tail_counters[next_index]);
        if (expected_term_id != aeron_logbuffer_term_id(raw_tail))
        {
            break;
        }
    }
    while (!aeron_logbuffer_cas_raw_tail(
        log_meta_data, next_index, raw_tail, (int64_t)((uint64_t)next_term_id << 32u)));

    return aeron_logbuffer_cas_active_term_count(log_meta_data, current_term_count, next_term_count);
}

inline void aeron_logbuffer_fill_default_header(
    uint8_t *log_meta_data_buffer, int32_t session_id, int32_t stream_id, int32_t initial_term_id)
{
    aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)log_meta_data_buffer;
    // todo: original
    //aeron_data_header_t *data_header =
    //    (aeron_data_header_t *)(log_meta_data_buffer + sizeof(aeron_logbuffer_metadata_t));
    aeron_data_header_t *data_header = (aeron_data_header_t *)(log_meta_data->default_header);

    log_meta_data->default_frame_header_length = AERON_DATA_HEADER_LENGTH;
    data_header->frame_header.frame_length = 0;
    data_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    data_header->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;
    data_header->stream_id = stream_id;
    data_header->session_id = session_id;
    data_header->term_id = initial_term_id;
    data_header->term_offset = 0;
    data_header->reserved_value = AERON_DATA_HEADER_DEFAULT_RESERVED_VALUE;
}

inline void aeron_logbuffer_apply_default_header(uint8_t *log_meta_data_buffer, uint8_t *buffer)
{
    aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)log_meta_data_buffer;
    //uint8_t *default_header = log_meta_data_buffer + sizeof(aeron_logbuffer_metadata_t);

    //memcpy(buffer, default_header, (size_t)log_meta_data->default_frame_header_length);

    memcpy(buffer, log_meta_data->default_header, (size_t)log_meta_data->default_frame_header_length);
}

inline size_t aeron_logbuffer_compute_fragmented_length(size_t length, size_t max_payload_length)
{
    const size_t num_max_payloads = length / max_payload_length;
    const size_t remaining_payload = length % max_payload_length;
    const size_t last_frame_length = (remaining_payload > 0) ?
        AERON_ALIGN(remaining_payload + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT) : 0;

    return (num_max_payloads * (max_payload_length + AERON_DATA_HEADER_LENGTH)) + last_frame_length;
}

#endif //AERON_LOGBUFFER_DESCRIPTOR_H
