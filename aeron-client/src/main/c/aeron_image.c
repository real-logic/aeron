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

#include <errno.h>

#include "aeron_image.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_image_create(
    aeron_image_t **image,
    aeron_client_conductor_t *conductor,
    aeron_log_buffer_t *log_buffer,
    int64_t *subscriber_position,
    int64_t correlation_id,
    int32_t session_id)
{
    aeron_image_t *_image;

    *image = NULL;
    if (aeron_alloc((void **)&_image, sizeof(aeron_image_t)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_image_create (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    _image->command_base.type = AERON_CLIENT_TYPE_IMAGE;

    _image->log_buffer = log_buffer;

    _image->subscriber_position = subscriber_position;

    _image->conductor = conductor;
    _image->correlation_id = correlation_id;
    _image->session_id = session_id;
    _image->removal_change_number = INT64_MAX;
    _image->is_closed = false;

    *image = _image;
    return 0;
}

int aeron_image_delete(aeron_image_t *image)
{
    aeron_free(image);

    return 0;
}

int aeron_image_poll(aeron_image_t *image, aeron_fragment_handler_t handler, void *clientd, int fragment_limit)
{
    return 0;
}

int aeron_image_controlled_poll(
    aeron_image_t *image, aeron_controlled_fragment_handler_t handler, void *clientd, int fragment_limit)
{
    return -1;
}

int aeron_image_bounded_poll(
    aeron_image_t *image, aeron_fragment_handler_t handler, void *clientd, int64_t limit_position, int fragment_limit)
{
    return -1;
}

int aeron_image_bounded_controlled_poll(
    aeron_image_t *image, aeron_controlled_fragment_handler_t handler,
    void *clientd, int64_t limit_position, int fragment_limit)
{
    return -1;
}

int64_t aeron_image_controlled_peek(
    aeron_image_t *image, aeron_controlled_fragment_handler_t handler, void *clientd, int64_t limit_position)
{
    return -1;
}

int aeron_image_block_poll(
    aeron_image_t *image, aeron_block_handler_t handler, void *clientd, size_t block_length_limit)
{
    return -1;
}

bool aeron_image_is_closed(aeron_image_t *image)
{
    bool is_closed = false;

    if (NULL != image)
    {
        AERON_GET_VOLATILE(is_closed, image->is_closed);
    }

    return is_closed;
}

extern int64_t aeron_image_removal_change_number(aeron_image_t *image);
extern bool aeron_image_is_in_use(aeron_image_t *image, int64_t last_change_number);
