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
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_image.h"

int aeron_subscription_create(
    aeron_subscription_t **subscription,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
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
    return -1;
}

int aeron_subscription_delete(aeron_subscription_t *subscription)
{
    aeron_free((void *)subscription->channel);
    aeron_free(subscription);
    return 0;
}

int aeron_subscription_close(aeron_subscription_t *subscription)
{
    return NULL != subscription ?
        aeron_client_conductor_async_close_subscription(subscription->conductor, subscription) : 0;
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

    if (-1 != image_index || 0 == old_length)
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
    int32_t last_change_number;

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
        }
    }

    return 0;
}

int aeron_subscription_poll(
    aeron_subscription_t *subscription, aeron_fragment_handler_t handler, void *clientd, int fragment_limit)
{
    volatile aeron_image_list_t *image_list;

    AERON_GET_VOLATILE(image_list, subscription->conductor_fields.image_lists_head.next_list);

    size_t length = image_list->length;
    int fragments_read = 0;
    size_t starting_index = subscription->round_robin_index++;
    if (starting_index >= length)
    {
        subscription->round_robin_index = starting_index = 0;
    }

    for (size_t i = starting_index; i < length && fragments_read < fragment_limit; i++)
    {
        fragments_read += aeron_image_poll(image_list->array[i], handler, clientd, fragment_limit - fragments_read);
    }

    for (size_t i = 0; i < starting_index && fragments_read < fragment_limit; i++)
    {
        fragments_read += aeron_image_poll(image_list->array[i], handler, clientd, fragment_limit - fragments_read);
    }

    if (image_list->change_number > subscription->last_image_list_change_number)
    {
        AERON_PUT_ORDERED(subscription->last_image_list_change_number, image_list->change_number);
    }

    return fragments_read;
}

extern int aeron_subscription_find_image_index(volatile aeron_image_list_t *image_list, aeron_image_t *image);
