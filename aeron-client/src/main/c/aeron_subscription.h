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

#ifndef AERON_C_SUBSCRIPTION_H
#define AERON_C_SUBSCRIPTION_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_image_list_stct
{
    uint32_t length;
    int32_t change_number;
    volatile struct aeron_image_list_stct *next_list;
    aeron_image_t **array;
}
aeron_image_list_t;

#define AERON_IMAGE_LIST_ALLOC_SIZE(l) (sizeof(aeron_image_list_t) + (l * sizeof(aeron_image_t *)))

typedef struct aeron_subscription_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;
    const char *channel;

    struct subscription_conductor_fields_stct
    {
        aeron_image_list_t image_lists_head;
        int32_t next_change_number;
    }
    conductor_fields;

    int64_t *channel_status_indicator;

    int64_t registration_id;
    int32_t stream_id;

    int32_t last_image_list_change_number;

    aeron_on_available_image_t on_available_image;
    void *on_available_image_clientd;
    aeron_on_unavailable_image_t on_unavailable_image;
    void *on_unavailable_image_clientd;

    size_t round_robin_index;
    bool is_closed;
}
aeron_subscription_t;

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
    void *on_unavailable_image_clientd);

int aeron_subscription_delete(aeron_subscription_t *subscription);

int aeron_subscription_alloc_image_list(volatile aeron_image_list_t **image_list, size_t length);

int aeron_client_conductor_subscription_add_image(aeron_subscription_t *subscription, aeron_image_t *image);
int aeron_client_conductor_subscription_remove_image(aeron_subscription_t *subscription, aeron_image_t *image);

int aeron_client_conductor_subscription_install_new_image_list(
    aeron_subscription_t *subscription, volatile aeron_image_list_t *image_list);

int aeron_client_conductor_subscription_prune_image_lists(aeron_subscription_t *subscription);

inline int aeron_subscription_find_image_index(volatile aeron_image_list_t *image_list, aeron_image_t *image)
{
    size_t length = (NULL == image_list) ? 0 : image_list->length;

    for (size_t i = 0; i < length; i++)
    {
        if (image == image_list->array[i])
        {
            return (int)i;
        }
    }

    return -1;
}

#endif //AERON_C_SUBSCRIPTION_H
