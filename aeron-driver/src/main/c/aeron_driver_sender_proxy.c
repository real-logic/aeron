/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "aeron_driver_sender_proxy.h"
#include "aeron_driver_sender.h"

void aeron_driver_sender_proxy_register_endpoint(
    aeron_driver_context_t *context, aeron_send_channel_endpoint_t *endpoint)
{
    if (AERON_THREADING_MODE_SHARED == context->threading_mode)
    {
        aeron_driver_sender_on_register_endpoint(context->sender, endpoint);
    }
    else
    {

    }
}
