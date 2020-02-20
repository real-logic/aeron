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
#include "util/aeron_error.h"

int aeron_udp_protocol_sm_receiver_tag(aeron_status_message_header_t *sm, int64_t *receiver_tag)
{
    const size_t receiver_tag_offset = sizeof(aeron_status_message_header_t) +
        offsetof(aeron_status_message_optional_header_t, receiver_tag);
    const size_t receiver_tag_size = sizeof(*receiver_tag);
    const size_t frame_length_with_receiver_tag = receiver_tag_offset + receiver_tag_size;

    if (sm->frame_header.frame_length == (int32_t)frame_length_with_receiver_tag)
    {
        const uint8_t *sm_ptr = (const uint8_t *)sm + receiver_tag_offset;
        memcpy(receiver_tag, sm_ptr, receiver_tag_size);

        return (int)receiver_tag_size;
    }

    *receiver_tag = 0;

    return (int)((size_t)sm->frame_header.frame_length - receiver_tag_offset);
}
