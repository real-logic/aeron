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
#include "aeron_log_buffer.h"
#include "aeron_subscription.h"

int aeron_image_create(
    aeron_image_t **image,
    aeron_subscription_t *subscription,
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

    _image->subscription = subscription;
    _image->log_buffer = log_buffer;

    _image->subscriber_position = subscriber_position;

    _image->conductor = conductor;
    _image->correlation_id = correlation_id;
    _image->session_id = session_id;
    _image->removal_change_number = INT64_MAX;
    _image->final_position = 0;
    _image->refcnt = 1;

    aeron_logbuffer_metadata_t *metadata =
        (aeron_logbuffer_metadata_t *)log_buffer->mapped_raw_log.log_meta_data.addr;
    int32_t term_length = metadata->term_length;

    _image->term_length_mask = term_length - 1;
    _image->position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes(term_length);

    _image->is_closed = false;
    _image->is_lingering = false;

    *image = _image;
    return 0;
}

int aeron_image_delete(aeron_image_t *image)
{
    aeron_free(image);

    return 0;
}

void aeron_image_force_close(aeron_image_t *image)
{
    AERON_PUT_ORDERED(image->is_closed, true);
}

int aeron_image_retain(aeron_image_t *image)
{
    if (NULL == image)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_retain(NULL): %s", strerror(EINVAL));
        return -1;
    }

    aeron_image_incr_refcnt(image);
    return 0;
}

int aeron_image_release(aeron_image_t *image)
{
    if (NULL == image)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_release(NULL): %s", strerror(EINVAL));
        return -1;
    }

    /*
     * Update the subscriptions last image change number so that if the subscription isn't polling or touching
     * or touched the image list, then at least this will allow the previous image_lists to be reclaimed.
     */
    int64_t last_change_number = aeron_subscription_last_image_list_change_number(image->subscription);
    aeron_subscription_propose_last_image_change_number(image->subscription, last_change_number);

    aeron_image_decr_refcnt(image);
    return 0;
}

int64_t aeron_image_position(aeron_image_t *image)
{
    bool is_closed;

    if (NULL == image)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_position(NULL): %s", strerror(EINVAL));
        return -1;
    }

    AERON_GET_VOLATILE(is_closed, image->is_closed);
    if (is_closed)
    {
        return image->final_position;
    }

    return *image->subscriber_position;
}

int aeron_image_set_position(aeron_image_t *image, int64_t position)
{
    bool is_closed;

    if (NULL == image)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_set_position(NULL): %s", strerror(EINVAL));
        return -1;
    }

    AERON_GET_VOLATILE(is_closed, image->is_closed);
    if (!is_closed)
    {
        if (aeron_image_validate_position(image, position) < 0)
        {
            return -1;
        }

        AERON_PUT_ORDERED(*image->subscriber_position, position);
    }

    return 0;
}

int aeron_image_poll(aeron_image_t *image, aeron_fragment_handler_t handler, void *clientd, size_t fragment_limit)
{
    bool is_closed;
    size_t fragments_read = 0;

    if (NULL == image || NULL == handler)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_poll(NULL): %s", strerror(EINVAL));
        return -1;
    }

    AERON_GET_VOLATILE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    const int64_t initial_position = *image->subscriber_position;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    int32_t offset = initial_offset;

    while (fragments_read < fragment_limit && offset < capacity)
    {
        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset;

        AERON_GET_VOLATILE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        if (AERON_HDR_TYPE_PAD != frame->frame_header.type)
        {
            aeron_header_t header = { frame };

            handler(
                clientd,
                term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
                frame_length - AERON_DATA_HEADER_LENGTH,
                &header);
            ++fragments_read;
        }
    }

    int64_t new_position = initial_position + (offset - initial_offset);
    if (new_position > initial_position)
    {
        aeron_counter_set_ordered(image->subscriber_position, new_position);
    }

    return (int)fragments_read;
}

int aeron_image_controlled_poll(
    aeron_image_t *image, aeron_controlled_fragment_handler_t handler, void *clientd, size_t fragment_limit)
{
    bool is_closed;
    size_t fragments_read = 0;

    if (NULL == image || NULL == handler)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_controlled_poll(NULL): %s", strerror(EINVAL));
        return -1;
    }

    AERON_GET_VOLATILE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    int64_t initial_position = *image->subscriber_position;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    int32_t offset = initial_offset;

    while (fragments_read < fragment_limit && offset < capacity)
    {
        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset, aligned_frame_length;

        AERON_GET_VOLATILE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
        offset += aligned_frame_length;

        if (AERON_HDR_TYPE_PAD == frame->frame_header.type)
        {
            continue;
        }

        aeron_header_t header = { frame };
        aeron_controlled_fragment_handler_action_t action =
            handler(
                clientd,
                term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
                frame_length - AERON_DATA_HEADER_LENGTH,
                &header);

        if (AERON_ACTION_ABORT == action)
        {
            offset -= aligned_frame_length;
            break;
        }

        ++fragments_read;

        if (AERON_ACTION_BREAK == action)
        {
            break;
        }
        else if (AERON_ACTION_COMMIT == action)
        {
            initial_position += (offset - initial_offset);
            initial_offset = offset;
            aeron_counter_set_ordered(image->subscriber_position, initial_position);
        }
    }

    int64_t new_position = initial_position + (offset - initial_offset);
    if (new_position > initial_position)
    {
        aeron_counter_set_ordered(image->subscriber_position, new_position);
    }

    return (int)fragments_read;
}

int aeron_image_bounded_poll(
    aeron_image_t *image, aeron_fragment_handler_t handler, void *clientd, int64_t limit_position, size_t fragment_limit)
{
    bool is_closed;
    size_t fragments_read = 0;

    if (NULL == image || NULL == handler)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_bounded_poll(NULL): %s", strerror(EINVAL));
        return -1;
    }

    AERON_GET_VOLATILE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    const int64_t initial_position = *image->subscriber_position;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    const int64_t high_limit_offset = limit_position - initial_position + initial_offset;
    const int32_t limit_offset = (int64_t)capacity < high_limit_offset ? capacity : (int32_t)high_limit_offset;
    int32_t offset = initial_offset;

    while (fragments_read < fragment_limit && offset < limit_offset)
    {
        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset;

        AERON_GET_VOLATILE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        if (AERON_HDR_TYPE_PAD != frame->frame_header.type)
        {
            aeron_header_t header = { frame };

            handler(
                clientd,
                term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
                frame_length - AERON_DATA_HEADER_LENGTH,
                &header);
            ++fragments_read;
        }
    }

    int64_t new_position = initial_position + (offset - initial_offset);
    if (new_position > initial_position)
    {
        aeron_counter_set_ordered(image->subscriber_position, new_position);
    }

    return (int)fragments_read;
}

int aeron_image_bounded_controlled_poll(
    aeron_image_t *image, aeron_controlled_fragment_handler_t handler,
    void *clientd, int64_t limit_position, size_t fragment_limit)
{
    bool is_closed;
    size_t fragments_read = 0;

    if (NULL == image || NULL == handler)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_bounded_controlled_poll(NULL): %s", strerror(EINVAL));
        return -1;
    }

    AERON_GET_VOLATILE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    int64_t initial_position = *image->subscriber_position;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    const int64_t high_limit_offset = limit_position - initial_position + initial_offset;
    const int32_t limit_offset = (int64_t)capacity < high_limit_offset ? capacity : (int32_t)high_limit_offset;
    int32_t offset = initial_offset;

    while (fragments_read < fragment_limit && offset < limit_offset)
    {
        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset, aligned_frame_length;

        AERON_GET_VOLATILE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
        offset += aligned_frame_length;

        if (AERON_HDR_TYPE_PAD == frame->frame_header.type)
        {
            continue;
        }

        aeron_header_t header = { frame };
        aeron_controlled_fragment_handler_action_t action =
            handler(
                clientd,
                term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
                frame_length - AERON_DATA_HEADER_LENGTH,
                &header);

        if (AERON_ACTION_ABORT == action)
        {
            offset -= aligned_frame_length;
            break;
        }

        ++fragments_read;

        if (AERON_ACTION_BREAK == action)
        {
            break;
        }
        else if (AERON_ACTION_COMMIT == action)
        {
            initial_position += (offset - initial_offset);
            initial_offset = offset;
            aeron_counter_set_ordered(image->subscriber_position, initial_position);
        }
    }

    int64_t new_position = initial_position + (offset - initial_offset);
    if (new_position > initial_position)
    {
        aeron_counter_set_ordered(image->subscriber_position, new_position);
    }

    return (int)fragments_read;
}

int64_t aeron_image_controlled_peek(
    aeron_image_t *image,
    int64_t initial_position,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    int64_t limit_position)
{
    bool is_closed;

    if (NULL == image || NULL == handler)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_controlled_peek(NULL): %s", strerror(EINVAL));
        return -1;
    }

    AERON_GET_VOLATILE(is_closed, image->is_closed);
    if (is_closed)
    {
        return initial_position;
    }

    if (aeron_image_validate_position(image, initial_position) < 0)
    {
        return -1;
    }

    if (initial_position >= limit_position)
    {
        return initial_position;
    }

    int64_t position = initial_position, resulting_position = initial_position;
    const size_t index = aeron_logbuffer_index_by_position(initial_position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    int32_t initial_offset = (int32_t)initial_position & image->term_length_mask;
    const int64_t high_limit_offset = limit_position - initial_position + initial_offset;
    const int32_t limit_offset = (int64_t)capacity < high_limit_offset ? capacity : (int32_t)high_limit_offset;
    int32_t offset = initial_offset;

    while (offset < limit_offset)
    {
        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + offset);
        int32_t frame_length, frame_offset;

        AERON_GET_VOLATILE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        frame_offset = offset;
        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        if (AERON_HDR_TYPE_PAD == frame->frame_header.type)
        {
            position += (offset - initial_offset);
            initial_offset = offset;
            resulting_position = position;

            continue;
        }

        aeron_header_t header = { frame };
        aeron_controlled_fragment_handler_action_t action =
            handler(
                clientd,
                term_buffer + frame_offset + AERON_DATA_HEADER_LENGTH,
                frame_length - AERON_DATA_HEADER_LENGTH,
                &header);

        if (AERON_ACTION_ABORT == action)
        {
            break;
        }

        position += (offset - initial_offset);
        initial_offset = offset;

        if (frame->frame_header.flags & AERON_DATA_HEADER_END_FLAG)
        {
            resulting_position = position;
        }

        if (AERON_ACTION_BREAK == action)
        {
            break;
        }
    }

    return resulting_position;
}

int aeron_image_block_poll(
    aeron_image_t *image, aeron_block_handler_t handler, void *clientd, size_t block_length_limit)
{
    bool is_closed;

    if (NULL == image || NULL == handler)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_image_block_poll(NULL): %s", strerror(EINVAL));
        return -1;
    }

    AERON_GET_VOLATILE(is_closed, image->is_closed);
    if (is_closed)
    {
        return 0;
    }

    const int64_t position = *image->subscriber_position;
    const size_t index = aeron_logbuffer_index_by_position(position, image->position_bits_to_shift);
    const uint8_t *term_buffer = image->log_buffer->mapped_raw_log.term_buffers[index].addr;
    const int32_t offset = (int32_t)position & image->term_length_mask;
    const int32_t capacity = (const int32_t)image->log_buffer->mapped_raw_log.term_length;
    const int64_t high_limit_offset = offset + block_length_limit;
    const int32_t limit_offset = (int64_t)capacity < high_limit_offset ? capacity : (int32_t)high_limit_offset;
    int32_t scan_offset = offset;

    while (scan_offset < limit_offset)
    {
        aeron_data_header_t *frame = (aeron_data_header_t *)(term_buffer + scan_offset);
        int32_t frame_length, aligned_frame_length;

        AERON_GET_VOLATILE(frame_length, frame->frame_header.frame_length);

        if (frame_length <= 0)
        {
            break;
        }

        aligned_frame_length = AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
        scan_offset += aligned_frame_length;

        if (AERON_HDR_TYPE_PAD == frame->frame_header.type)
        {
            if (offset == scan_offset)
            {
                scan_offset += aligned_frame_length;
            }

            break;
        }

        if (scan_offset + aligned_frame_length > limit_offset)
        {
            break;
        }

        scan_offset += aligned_frame_length;
    }

    int32_t resulting_offset = scan_offset;
    int32_t length = resulting_offset - offset;

    if (resulting_offset > offset)
    {
        int32_t term_id = ((aeron_data_header_t *)(term_buffer + offset))->term_id;

        handler(
            clientd,
            term_buffer + offset,
            length,
            image->session_id,
            term_id);
    }

    aeron_counter_set_ordered(image->subscriber_position, position + length);

    return (int)length;
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
extern bool aeron_image_is_in_use_by_subcription(aeron_image_t *image, int64_t last_change_number);
extern int aeron_image_validate_position(aeron_image_t *image, int64_t position);
extern int64_t aeron_image_incr_refcnt(aeron_image_t *image);
extern int64_t aeron_image_decr_refcnt(aeron_image_t *image);
extern int64_t aeron_image_refcnt_volatile(aeron_image_t *image);
