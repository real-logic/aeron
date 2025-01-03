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
#include "concurrent/aeron_term_unblocker.h"

void aeron_term_unblocker_reset_header(
    aeron_data_header_t *data_header,
    aeron_logbuffer_metadata_t *log_meta_data,
    int32_t term_offset,
    int32_t term_id,
    int32_t frame_length)
{
    aeron_logbuffer_apply_default_header((uint8_t *)log_meta_data, (uint8_t *)data_header);
    data_header->frame_header.type = AERON_HDR_TYPE_PAD;
    data_header->term_offset = term_offset;
    data_header->term_id = term_id;
    AERON_SET_RELEASE(data_header->frame_header.frame_length, frame_length);
}

bool aeron_term_unblocker_scan_back_to_confirm_zeroed(
    uint8_t *buffer,
    int32_t from,
    int32_t limit)
{
    int32_t i = from - AERON_LOGBUFFER_FRAME_ALIGNMENT;
    bool all_zeroes = true;

    while (i >= limit)
    {
        aeron_frame_header_t *frame_header = (aeron_frame_header_t *)(buffer + i);
        int32_t frame_length;

        AERON_GET_ACQUIRE(frame_length, frame_header->frame_length);

        if (0 != frame_length)
        {
            all_zeroes = false;
            break;
        }

        i -= AERON_LOGBUFFER_FRAME_ALIGNMENT;
    }

    return all_zeroes;
}

aeron_term_unblocker_status_t aeron_term_unblocker_unblock(
    aeron_logbuffer_metadata_t *log_meta_data,
    uint8_t *buffer,
    size_t term_length,
    int32_t blocked_offset,
    int32_t tail_offset,
    int32_t term_id)
{
    aeron_term_unblocker_status_t status = AERON_TERM_UNBLOCKER_STATUS_NO_ACTION;
    aeron_data_header_t *data_header = (aeron_data_header_t *)(buffer + blocked_offset);
    int32_t frame_length;

    AERON_GET_ACQUIRE(frame_length, data_header->frame_header.frame_length);
    if (frame_length < 0)
    {
        aeron_term_unblocker_reset_header(data_header, log_meta_data, blocked_offset, term_id, -frame_length);
        status = AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED;
    }
    else if (0 == frame_length)
    {
        int32_t current_offset = blocked_offset + AERON_LOGBUFFER_FRAME_ALIGNMENT;

        while (current_offset < tail_offset)
        {
            data_header = (aeron_data_header_t *)(buffer + current_offset);
            AERON_GET_ACQUIRE(frame_length, data_header->frame_header.frame_length);
            if (frame_length != 0)
            {
                if (aeron_term_unblocker_scan_back_to_confirm_zeroed(buffer, current_offset, blocked_offset))
                {
                    const int32_t length = current_offset - blocked_offset;
                    aeron_term_unblocker_reset_header(
                        (aeron_data_header_t *)(buffer + blocked_offset), log_meta_data, blocked_offset, term_id, length);
                    status = AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED;
                }
                break;
            }

            current_offset += AERON_LOGBUFFER_FRAME_ALIGNMENT;
        }

        if (current_offset == (int32_t)term_length)
        {
            data_header = (aeron_data_header_t *)(buffer + blocked_offset);
            AERON_GET_ACQUIRE(frame_length, data_header->frame_header.frame_length);
            if (0 == frame_length)
            {
                const int32_t length = current_offset - blocked_offset;
                aeron_term_unblocker_reset_header(data_header, log_meta_data, blocked_offset, term_id, length);
                status = AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED_TO_END;
            }
        }
    }

    return status;
}
