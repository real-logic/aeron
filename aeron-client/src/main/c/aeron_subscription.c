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

int aeron_subscription_create(
    aeron_subscription_t **subscription,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    aeron_on_available_image_t on_available_image,
    aeron_on_unavailable_image_t on_unavailable_image)
{
    aeron_subscription_t *_subscription;

    *subscription = NULL;
    if (aeron_alloc((void **)&_subscription, sizeof(aeron_subscription_t)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_subscription_create (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    _subscription->conductor = conductor;
    _subscription->channel = channel;
    _subscription->registration_id = registration_id;
    _subscription->stream_id = stream_id;
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
