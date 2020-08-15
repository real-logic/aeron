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

#include "aeron_subscription.h"
#include "aeron_image.h"

int aeron_subscription_create(
    aeron_subscription_t **subscription,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t channel_status_indicator_id,
    int64_t *channel_status_indicator_addr,
    aeron_on_available_image_t on_available_image,
    void *on_available_image_clientd,
    aeron_on_unavailable_image_t on_unavailable_image,
    void *on_unavailable_image_clientd)
{
    aeron_subscription_t *_subscription;

    *subscription = NULL;
    if (aeron_alloc((void **)&_subscription, sizeof(aeron_subscription_t)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_subscription_create (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    _subscription->command_base.type = AERON_CLIENT_TYPE_SUBSCRIPTION;

    _subscription->conductor_fields.image_lists_head.next_list = NULL;
    _subscription->conductor_fields.next_change_number = 0;
    _subscription->last_image_list_change_number = -1;

    if (aeron_subscription_alloc_image_list(&_subscription->conductor_fields.image_lists_head.next_list, 0) < 0)
    {
        return -1;
    }

    _subscription->channel_status_indicator_id = channel_status_indicator_id;
    _subscription->channel_status_indicator = channel_status_indicator_addr;

    _subscription->conductor = conductor;
    _subscription->channel = channel;
    _subscription->registration_id = registration_id;
    _subscription->stream_id = stream_id;
    _subscription->on_available_image = on_available_image;
    _subscription->on_available_image_clientd = on_available_image_clientd;
    _subscription->on_unavailable_image = on_unavailable_image;
    _subscription->on_unavailable_image_clientd = on_unavailable_image_clientd;

    _subscription->round_robin_index = 0;
    _subscription->is_closed = false;

    *subscription = _subscription;

    return 0;
}

int aeron_subscription_delete(aeron_subscription_t *subscription)
{
    volatile aeron_image_list_t *prune_lists_head = &subscription->conductor_fields.image_lists_head;

    while (NULL != prune_lists_head->next_list)
    {
        volatile aeron_image_list_t *prune_list = prune_lists_head->next_list;

        prune_lists_head->next_list = prune_list->next_list;
        aeron_free((void *)prune_list);
    }

    aeron_free((void *)subscription->channel);
    aeron_free(subscription);

    return 0;
}

void aeron_subscription_force_close(aeron_subscription_t *subscription)
{
    AERON_PUT_ORDERED(subscription->is_closed, true);
}

int aeron_subscription_close(
    aeron_subscription_t *subscription, aeron_notification_t on_close_complete, void *on_close_complete_clientd)
{
    if (NULL != subscription)
    {
        bool is_closed;

        AERON_GET_VOLATILE(is_closed, subscription->is_closed);
        if (!is_closed)
        {
            AERON_PUT_ORDERED(subscription->is_closed, true);
            if (aeron_client_conductor_async_close_subscription(
                subscription->conductor, subscription, on_close_complete, on_close_complete_clientd) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int aeron_subscription_alloc_image_list(volatile aeron_image_list_t **image_list, size_t length)
{
    aeron_image_list_t *_image_list;

    *image_list = NULL;
    if (aeron_alloc((void **)&_image_list, AERON_IMAGE_LIST_ALLOC_SIZE(length)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_subscription_create (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    _image_list->change_number = -1;
    _image_list->array = 0 == length ? NULL : (aeron_image_t **)((uint8_t *)_image_list + sizeof(aeron_image_list_t));
    _image_list->length = (uint32_t)length;
    _image_list->next_list = NULL;

    *image_list = _image_list;

    return 0;
}

int aeron_client_conductor_subscription_add_image(aeron_subscription_t *subscription, aeron_image_t *image)
{
    volatile aeron_image_list_t *current_image_list = subscription->conductor_fields.image_lists_head.next_list;
    volatile aeron_image_list_t *new_image_list;
    size_t old_length = current_image_list->length;

    if (aeron_subscription_alloc_image_list(&new_image_list, old_length + 1) < 0)
    {
        return -1;
    }

    for (size_t i = 0; i < old_length; i++)
    {
        new_image_list->array[i] = current_image_list->array[i];
    }

    new_image_list->array[old_length] = image;

    return aeron_client_conductor_subscription_install_new_image_list(subscription, new_image_list);
}

int aeron_client_conductor_subscription_remove_image(aeron_subscription_t *subscription, aeron_image_t *image)
{
    volatile aeron_image_list_t *current_image_list = subscription->conductor_fields.image_lists_head.next_list;
    volatile aeron_image_list_t *new_image_list;
    size_t old_length = current_image_list->length;
    int image_index = aeron_subscription_find_image_index(current_image_list, image);

    if (-1 == image_index || 0 == old_length)
    {
        return 0;
    }

    if (aeron_subscription_alloc_image_list(&new_image_list, old_length - 1) < 0)
    {
        return -1;
    }

    size_t j = 0;
    for (size_t i = 0; i < old_length; i++)
    {
        if (image != current_image_list->array[i])
        {
            new_image_list->array[j++] = current_image_list->array[i];
        }
    }

    image->removal_change_number = subscription->conductor_fields.next_change_number;

    return aeron_client_conductor_subscription_install_new_image_list(subscription, new_image_list);
}

int aeron_client_conductor_subscription_install_new_image_list(
    aeron_subscription_t *subscription, volatile aeron_image_list_t *image_list)
{
    /*
     * Called from the client conductor to add/remove images to the image list. A new image list is passed each time.
     */
    image_list->change_number = subscription->conductor_fields.next_change_number++;
    image_list->next_list = subscription->conductor_fields.image_lists_head.next_list;

    AERON_PUT_ORDERED(subscription->conductor_fields.image_lists_head.next_list, image_list);

    return 0;
}

int aeron_client_conductor_subscription_prune_image_lists(aeron_subscription_t *subscription)
{
    /*
     * Called from the client conductor to prune old image lists and free them up. Does not free Images.
     */
    volatile aeron_image_list_t *prune_lists_head = &subscription->conductor_fields.image_lists_head;
    int64_t last_change_number;
    int pruned_lists_count = 0;

    AERON_GET_VOLATILE(last_change_number, subscription->last_image_list_change_number);

    while (NULL != prune_lists_head->next_list)
    {
        if (prune_lists_head->next_list->change_number >= last_change_number)
        {
            prune_lists_head = prune_lists_head->next_list;
        }
        else
        {
            volatile aeron_image_list_t *prune_list = prune_lists_head->next_list;

            prune_lists_head->next_list = prune_list->next_list;
            aeron_free((void *)prune_list);
            pruned_lists_count++;
        }
    }

    return pruned_lists_count;
}

bool aeron_subscription_is_connected(aeron_subscription_t *subscription)
{
    volatile aeron_image_list_t *image_list;
    bool result = false;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    for (size_t i = 0, length = image_list->length; i < length; i++)
    {
        if (!aeron_image_is_closed(image_list->array[i]))
        {
            result = true;
            break;
        }
    }

    aeron_subscription_propose_last_image_change_number(subscription, image_list->change_number);

    return result;
}

int aeron_subscription_constants(aeron_subscription_t *subscription, aeron_subscription_constants_t *constants)
{
    if (NULL == subscription || NULL == constants)
    {
        aeron_set_err(EINVAL, "%s", strerror(EINVAL));
        return -1;
    }

    constants->channel = subscription->channel;
    constants->registration_id = subscription->registration_id;
    constants->stream_id = subscription->stream_id;
    constants->on_available_image = subscription->on_available_image;
    constants->on_unavailable_image = subscription->on_unavailable_image;
    constants->channel_status_indicator_id = subscription->channel_status_indicator_id;

    return 0;
}

int aeron_subscription_image_count(aeron_subscription_t *subscription)
{
    volatile aeron_image_list_t *image_list;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    return image_list->length;
}

aeron_image_t *aeron_subscription_image_by_session_id(aeron_subscription_t *subscription, int32_t session_id)
{
    volatile aeron_image_list_t *image_list;
    aeron_image_t *result = NULL;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    for (size_t i = 0, length = image_list->length; i < length; i++)
    {
        if (session_id == image_list->array[i]->session_id)
        {
            result = image_list->array[i];
            break;
        }
    }

    if (NULL != result)
    {
        aeron_image_incr_refcnt(result);
    }

    aeron_subscription_propose_last_image_change_number(subscription, image_list->change_number);

    return result;
}

aeron_image_t *aeron_subscription_image_at_index(aeron_subscription_t *subscription, size_t index)
{
    volatile aeron_image_list_t *image_list;
    aeron_image_t *result = NULL;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    if (index < image_list->length)
    {
        result = image_list->array[index];
        aeron_image_incr_refcnt(result);
    }

    aeron_subscription_propose_last_image_change_number(subscription, image_list->change_number);

    return result;
}

void aeron_subscription_for_each_image(
    aeron_subscription_t *subscription, void (*handler)(aeron_image_t *image, void *clientd), void *clientd)
{
    volatile aeron_image_list_t *image_list;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    for (size_t i = 0, length = image_list->length; i < length; i++)
    {
        aeron_image_t *image = image_list->array[i];

        aeron_image_incr_refcnt(image);
        handler(image_list->array[i], clientd);
        aeron_image_decr_refcnt(image);
    }

    aeron_subscription_propose_last_image_change_number(subscription, image_list->change_number);
}

int aeron_subscription_image_retain(aeron_subscription_t *subscription, aeron_image_t *image)
{
    if (NULL == subscription || NULL == image)
    {
        aeron_set_err(EINVAL, "aeron_subscription_image_retain: %s", strerror(EINVAL));
        return -1;
    }

    /*
    * Update the subscriptions last image change number so that if the subscription isn't polling or touching
    * or touched the image list, then at least this will allow the previous image_lists to be reclaimed.
    */
    aeron_subscription_propose_last_image_change_number(
        subscription, aeron_subscription_last_image_list_change_number(subscription));

    aeron_image_incr_refcnt(image);

    return 0;
}

int aeron_subscription_image_release(aeron_subscription_t *subscription, aeron_image_t *image)
{
    if (NULL == subscription || NULL == image)
    {
        aeron_set_err(EINVAL, "aeron_subscription_image_release: %s", strerror(EINVAL));
        return -1;
    }

    /*
     * Update the subscriptions last image change number so that if the subscription isn't polling or touching
     * or touched the image list, then at least this will allow the previous image_lists to be reclaimed.
     */
    aeron_subscription_propose_last_image_change_number(
        subscription, aeron_subscription_last_image_list_change_number(subscription));

    aeron_image_decr_refcnt(image);

    return 0;
}

bool aeron_subscription_is_closed(aeron_subscription_t *subscription)
{
    bool is_closed = false;

    if (NULL != subscription)
    {
        AERON_GET_VOLATILE(is_closed, subscription->is_closed);
    }

    return is_closed;
}

int64_t aeron_subscription_channel_status(aeron_subscription_t *subscription)
{
    if (NULL != subscription && !aeron_subscription_is_closed(subscription))
    {
        int64_t value;
        AERON_GET_VOLATILE(value, *subscription->channel_status_indicator);

        return value;
    }

    return AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_NO_ID_ALLOCATED;
}

int aeron_subscription_poll(
    aeron_subscription_t *subscription, aeron_fragment_handler_t handler, void *clientd, size_t fragment_limit)
{
    volatile aeron_image_list_t *image_list;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    size_t length = image_list->length;
    size_t fragments_read = 0;
    size_t starting_index = subscription->round_robin_index++;
    if (starting_index >= length)
    {
        subscription->round_robin_index = starting_index = 0;
    }

    for (size_t i = starting_index; i < length && fragments_read < fragment_limit; i++)
    {
        fragments_read += (size_t)aeron_image_poll(
            image_list->array[i], handler, clientd, fragment_limit - fragments_read);
    }

    for (size_t i = 0; i < starting_index && fragments_read < fragment_limit; i++)
    {
        fragments_read += (size_t)aeron_image_poll(
            image_list->array[i], handler, clientd, fragment_limit - fragments_read);
    }

    aeron_subscription_propose_last_image_change_number(subscription, image_list->change_number);

    return (int)fragments_read;
}

int aeron_subscription_controlled_poll(
    aeron_subscription_t *subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit)
{
    volatile aeron_image_list_t *image_list;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    size_t length = image_list->length;
    size_t fragments_read = 0;
    size_t starting_index = subscription->round_robin_index++;
    if (starting_index >= length)
    {
        subscription->round_robin_index = starting_index = 0;
    }

    for (size_t i = starting_index; i < length && fragments_read < fragment_limit; i++)
    {
        fragments_read += (size_t)aeron_image_controlled_poll(
            image_list->array[i], handler, clientd, fragment_limit - fragments_read);
    }

    for (size_t i = 0; i < starting_index && fragments_read < fragment_limit; i++)
    {
        fragments_read += (size_t)aeron_image_controlled_poll(
            image_list->array[i], handler, clientd, fragment_limit - fragments_read);
    }

    aeron_subscription_propose_last_image_change_number(subscription, image_list->change_number);

    return (int)fragments_read;
}

long aeron_subscription_block_poll(
    aeron_subscription_t *subscription, aeron_block_handler_t handler, void *clientd, size_t block_length_limit)
{
    volatile aeron_image_list_t *image_list;
    long bytes_consumed = 0;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    for (size_t i = 0, length = image_list->length; i < length; i++)
    {
        bytes_consumed += aeron_image_block_poll(
            image_list->array[i], handler, clientd, block_length_limit);
    }

    aeron_subscription_propose_last_image_change_number(subscription, image_list->change_number);

    return bytes_consumed;
}

int aeron_header_values(aeron_header_t *header, aeron_header_values_t *values)
{
    if (NULL == header || NULL == values)
    {
        aeron_set_err(EINVAL, "%s", strerror(EINVAL));
        return -1;
    }

    memcpy(&values->frame, header->frame, sizeof(aeron_header_values_frame_t));
    values->initial_term_id = header->initial_term_id;

    return 0;
}

int64_t aeron_header_position(aeron_header_t *header)
{
    const int32_t offset_at_end_of_frame = AERON_ALIGN(
        header->frame->term_offset + header->frame->frame_header.frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    return aeron_logbuffer_compute_position(
        header->frame->term_id, offset_at_end_of_frame, header->position_bits_to_shift, header->initial_term_id);
}

extern int aeron_subscription_find_image_index(volatile aeron_image_list_t *image_list, aeron_image_t *image);
extern int64_t aeron_subscription_last_image_list_change_number(aeron_subscription_t *subscription);
extern void aeron_subscription_propose_last_image_change_number(
    aeron_subscription_t *subscription, int64_t change_number);
extern volatile aeron_image_list_t *aeron_client_conductor_subscription_image_list(aeron_subscription_t *subscription);
