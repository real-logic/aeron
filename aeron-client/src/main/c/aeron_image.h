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

#ifndef AERON_C_IMAGE_H
#define AERON_C_IMAGE_H

#include <inttypes.h>

#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_image_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;
    char *source_identity;

    aeron_subscription_t *subscription;
    aeron_log_buffer_t *log_buffer;
    aeron_logbuffer_metadata_t *metadata;

    int64_t *subscriber_position;

    int64_t correlation_id;
    int64_t removal_change_number;
    int64_t join_position;
    int64_t final_position;
    int64_t refcnt;

    int32_t session_id;
    int32_t term_length_mask;

    int32_t subscriber_position_id;

    size_t position_bits_to_shift;

    bool is_closed;
    bool is_eos;
    bool is_lingering;
}
aeron_image_t;

typedef struct aeron_header_stct
{
    aeron_data_header_t *frame;
    int32_t initial_term_id;
    size_t position_bits_to_shift;
}
aeron_header_t;

int aeron_image_create(
    aeron_image_t **image,
    aeron_subscription_t *subscription,
    aeron_client_conductor_t *conductor,
    aeron_log_buffer_t *log_buffer,
    int32_t subscriber_position_id,
    int64_t *subscriber_position,
    int64_t correlation_id,
    int32_t session_id,
    const char *source_identity,
    size_t source_identity_length);

int aeron_image_delete(aeron_image_t *image);
void aeron_image_force_close(aeron_image_t *image);

inline int64_t aeron_image_removal_change_number(aeron_image_t *image)
{
    return image->removal_change_number;
}

inline bool aeron_image_is_in_use_by_subscription(aeron_image_t *image, int64_t last_change_number)
{
    return image->removal_change_number > last_change_number;
}

inline int aeron_image_validate_position(aeron_image_t *image, int64_t position)
{
    const int64_t current_position = *image->subscriber_position;
    const int64_t limit_position =
        (current_position - (current_position & image->term_length_mask)) + image->term_length_mask + 1;

    if (position < current_position ||  position > limit_position)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "%s: %" PRId64 " position out of range %" PRId64 "-%" PRId64,
            strerror(EINVAL), position, current_position, limit_position);
        return -1;
    }

    if (0 != (position & (AERON_LOGBUFFER_FRAME_ALIGNMENT - 1)))
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "%s: position not aligned to FRAME_ALIGNMENT", strerror(EINVAL));
        return -1;
    }

    return 0;
}

inline int64_t aeron_image_incr_refcnt(aeron_image_t *image)
{
    return aeron_get_and_add_int64(&(image->refcnt), 1);
}

inline int64_t aeron_image_decr_refcnt(aeron_image_t *image)
{
    return aeron_get_and_add_int64(&image->refcnt, -1);
}

inline int64_t aeron_image_refcnt_volatile(aeron_image_t *image)
{
    int64_t value;
    AERON_GET_VOLATILE(value, image->refcnt);
    return value;
}

#endif //AERON_C_IMAGE_H
