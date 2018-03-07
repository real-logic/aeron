/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#ifndef AERON_AERON_UDP_PROTOCOL_H
#define AERON_AERON_UDP_PROTOCOL_H

#include <stdint.h>

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_frame_header_stct
{
    int32_t frame_length;
    int8_t version;
    uint8_t flags;
    int16_t type;
}
aeron_frame_header_t;

typedef struct aeron_setup_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t term_offset;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    int32_t active_term_id;
    int32_t term_length;
    int32_t mtu;
    int32_t ttl;
}
aeron_setup_header_t;

typedef struct aeron_data_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t term_offset;
    int32_t session_id;
    int32_t stream_id;
    int32_t term_id;
    int64_t reserved_value;
}
aeron_data_header_t;

typedef struct aeron_nak_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t session_id;
    int32_t stream_id;
    int32_t term_id;
    int32_t term_offset;
    int32_t length;
}
aeron_nak_header_t;

typedef struct aeron_status_message_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t session_id;
    int32_t stream_id;
    int32_t consumption_term_id;
    int32_t consumption_term_offset;
    int32_t receiver_window;
    int64_t receiver_id;
}
aeron_status_message_header_t;

typedef struct aeron_rttm_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t session_id;
    int32_t stream_id;
    int64_t echo_timestamp;
    int64_t reception_delta;
    int64_t receiver_id;
}
aeron_rttm_header_t;
#pragma pack(pop)

#define AERON_FRAME_HEADER_VERSION (0)

#define AERON_HDR_TYPE_PAD (0x00)
#define AERON_HDR_TYPE_DATA (0x01)
#define AERON_HDR_TYPE_NAK (0x02)
#define AERON_HDR_TYPE_SM (0x03)
#define AERON_HDR_TYPE_ERR (0x04)
#define AERON_HDR_TYPE_SETUP (0x05)
#define AERON_HDR_TYPE_RTTM (0x06)
#define AERON_HDR_TYPE_EXT (0xFFFF)

#define AERON_DATA_HEADER_LENGTH (sizeof(aeron_data_header_t))

#define AERON_DATA_HEADER_BEGIN_FLAG ((uint8_t)(0x80))
#define AERON_DATA_HEADER_END_FLAG ((uint8_t)(0x40))
#define AERON_DATA_HEADER_EOS_FLAG ((uint8_t)(0x20))

#define AERON_DATA_HEADER_DEFAULT_RESERVED_VALUE (0L)

#define AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG ((uint8_t)(0x80))

#define AERON_RTTM_HEADER_REPLY_FLAG ((uint8_t)(0x80))

#endif //AERON_AERON_UDP_PROTOCOL_H
