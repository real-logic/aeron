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

#ifndef AERON_TERM_GAP_SCANNER_H
#define AERON_TERM_GAP_SCANNER_H

#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_bitutil.h"
#include "aeron_logbuffer_descriptor.h"

typedef void (*aeron_term_gap_scanner_on_gap_detected_func_t)(void *clientd, int32_t term_id, int32_t term_offset, size_t length);

inline int32_t aeron_term_gap_scanner_scan_for_gap(
    const uint8_t *buffer,
    int32_t term_id,
    int32_t term_offset,
    int32_t limit_offset,
    aeron_term_gap_scanner_on_gap_detected_func_t on_gap_detected,
    void *clientd)
{
    int32_t offset = term_offset;

    do
    {
        aeron_frame_header_t *hdr = (aeron_frame_header_t *)(buffer + offset);
        int32_t frame_length;

        AERON_GET_ACQUIRE(frame_length, hdr->frame_length);
        if (frame_length <= 0)
        {
            break;
        }

        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    }
    while (offset < limit_offset);

    const int32_t gap_begin_offset = offset;
    if (offset < limit_offset)
    {
        offset += AERON_DATA_HEADER_LENGTH;
        while (offset < limit_offset)
        {
            aeron_frame_header_t *hdr = (aeron_frame_header_t *)(buffer + offset);
            int32_t frame_length;
            AERON_GET_ACQUIRE(frame_length, hdr->frame_length);

            if (0 != frame_length)
            {
                break;
            }
            offset += AERON_DATA_HEADER_LENGTH;
        }

        const size_t gap_length = offset - gap_begin_offset;
        on_gap_detected(clientd, term_id, gap_begin_offset, gap_length);
    }

    return gap_begin_offset;
}

#endif //AERON_TERM_GAP_SCANNER_H
