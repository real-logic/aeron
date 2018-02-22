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

#ifndef AERON_AERON_TERM_REBUILDER_H
#define AERON_AERON_TERM_REBUILDER_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include "protocol/aeron_udp_protocol.h"
#include "aeron_atomic.h"

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_data_header_as_longs_stct
{
    uint64_t hdr[4];
}
aeron_data_header_as_longs_t;
#pragma pack(pop)

inline void aeron_term_rebuilder_insert(uint8_t *dest, const uint8_t *src, size_t length)
{
    aeron_data_header_t *hdr_dest = (aeron_data_header_t *)dest;
    aeron_data_header_as_longs_t *dest_hdr_as_longs = (aeron_data_header_as_longs_t *)dest;
    aeron_data_header_as_longs_t *src_hdr_as_longs = (aeron_data_header_as_longs_t *)src;

    if (0 == hdr_dest->frame_header.frame_length)
    {
        memcpy(dest + AERON_DATA_HEADER_LENGTH, src + AERON_DATA_HEADER_LENGTH, length - AERON_DATA_HEADER_LENGTH);

        dest_hdr_as_longs->hdr[3] = src_hdr_as_longs->hdr[3];
        dest_hdr_as_longs->hdr[2] = src_hdr_as_longs->hdr[2];
        dest_hdr_as_longs->hdr[1] = src_hdr_as_longs->hdr[1];

        AERON_PUT_ORDERED(dest_hdr_as_longs->hdr[0], src_hdr_as_longs->hdr[0]);
    }
}

#endif //AERON_AERON_TERM_REBUILDER_H
