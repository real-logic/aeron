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

#include <stddef.h>
#include <string.h>

#include "aeron_udp_protocol.h"

int aeron_udp_protocol_group_tag(aeron_status_message_header_t *sm, int64_t *group_tag)
{
    const size_t group_tag_offset = sizeof(aeron_status_message_header_t) +
        offsetof(aeron_status_message_optional_header_t, group_tag);
    const size_t group_tag_size = sizeof(*group_tag);
    const size_t frame_length_with_group_tag = group_tag_offset + group_tag_size;

    if (sm->frame_header.frame_length == (int32_t)frame_length_with_group_tag)
    {
        const uint8_t *sm_ptr = (const uint8_t *)sm + group_tag_offset;
        memcpy(group_tag, sm_ptr, group_tag_size);

        return (int)group_tag_size;
    }

    *group_tag = 0;

    return (int)((size_t)sm->frame_header.frame_length - group_tag_offset);
}
