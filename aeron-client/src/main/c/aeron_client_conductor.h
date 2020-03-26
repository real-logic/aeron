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

#ifndef AERON_C_CLIENT_CONDUCTOR_H
#define AERON_C_CLIENT_CONDUCTOR_H

#include "aeronc.h"

typedef enum aeron_client_registration_status_en
{
    AERON_CLIENT_AWAITING_MEDIA_DRIVER,
    AERON_CLIENT_REGISTERED_MEDIA_DRIVER,
    AERON_CLIENT_ERRORED_MEDIA_DRIVER
}
aeron_client_registration_status_t;

typedef struct aeron_client_registering_resource_stct
{
    char *error_message;
    char *channel;
    char *filename;
    int64_t registration_id;
    int64_t original_registration_id;
    long long time_of_registration_ms;
    int32_t error_code;
    int32_t session_id;
    int32_t stream_id;
    aeron_client_registration_status_t registration_status;
}
aeron_client_registering_resource_t;

typedef enum aeron_client_managed_resource_type_en
{
    AERON_CLIENT_TYPE_PUBLICATION,
    AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION,
    AERON_CLIENT_TYPE_SUBSCRIPTION,
    AERON_CLIENT_TYPE_IMAGE
}
aeron_client_managed_resource_type_t;

typedef struct aeron_client_managed_resource_stct
{
    union aeron_client_managed_resource_un
    {
        aeron_publication_t *publication;
        aeron_exclusive_publication_t *exclusive_publication;
        aeron_subscription_t *subscription;
        aeron_image_t *image;
    }
    resource;
    aeron_client_managed_resource_type_t type;
    int64_t registration_id;
}
aeron_client_managed_resource_t;

typedef struct aeron_client_conductor_stct
{
    struct lingering_resources_stct
    {
        size_t length;
        size_t capacity;
        aeron_client_managed_resource_t *array;
    }
    lingering_resources;

    struct active_resources_stct
    {
        size_t length;
        size_t capacity;
        aeron_client_managed_resource_t *array;
    }
    active_resources;

    struct registering_resources_stct
    {
        size_t length;
        size_t capacity;
        aeron_client_registering_resource_t *array;
    }
    registering_resources;

    aeron_clock_func_t nano_clock;
    aeron_clock_func_t epoch_clock;
}
aeron_client_conductor_t;

int aeron_client_conductor_init(aeron_client_conductor_t *conductor, aeron_context_t *context);
int aeron_client_conductor_do_work(aeron_client_conductor_t *conductor);
void aeron_client_conductor_on_close(aeron_client_conductor_t *conductor);

int64_t aeron_client_conductor_add_publication(aeron_t *client, const char *uri);
int aeron_client_conductor_find_publication(
    aeron_publication_t **publication, aeron_t *client, int64_t registration_id);

#endif //AERON_C_CLIENT_CONDUCTOR_H
