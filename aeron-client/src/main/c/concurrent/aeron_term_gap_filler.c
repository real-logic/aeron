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

#include "concurrent/aeron_term_gap_filler.h"

bool aeron_term_gap_filler_try_fill_gap(
    aeron_logbuffer_metadata_t *log_meta_data,
    uint8_t *buffer,
    int32_t term_id,
    int32_t gap_offset,
    int32_t gap_length)
{
    int32_t offset = (gap_offset + gap_length) - AERON_LOGBUFFER_FRAME_ALIGNMENT;

    while (offset >= gap_offset)
    {
        aeron_frame_header_t *frame_header = (aeron_frame_header_t *)(buffer + offset);
        if (0 != frame_header->frame_length)
        {
            return false;
        }

        offset -= AERON_LOGBUFFER_FRAME_ALIGNMENT;
    }

    aeron_logbuffer_apply_default_header((uint8_t *)log_meta_data, buffer + gap_offset);
    aeron_data_header_t *data_header = (aeron_data_header_t *)(buffer + gap_offset);

    data_header->frame_header.type = AERON_HDR_TYPE_PAD;
    data_header->term_offset = gap_offset;
    data_header->term_id = term_id;
    AERON_SET_RELEASE(data_header->frame_header.frame_length, gap_length);

    return true;
}
