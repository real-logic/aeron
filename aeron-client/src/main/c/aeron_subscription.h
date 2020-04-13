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

typedef struct aeron_subscription_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;
    const char *channel;

    int64_t registration_id;
    int32_t stream_id;

    aeron_on_available_image_t on_available_image;
    aeron_on_unavailable_image_t on_unavailable_image;

    bool is_closed;
}
aeron_subscription_t;

int aeron_subscription_create(
    aeron_subscription_t **subscription,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    aeron_on_available_image_t on_available_image,
    aeron_on_unavailable_image_t on_unavailable_image);

int aeron_subscription_delete(aeron_subscription_t *subscription);

#endif //AERON_C_SUBSCRIPTION_H
