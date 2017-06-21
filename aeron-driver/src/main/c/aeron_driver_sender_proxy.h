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

#ifndef AERON_AERON_DRIVER_SENDER_PROXY_H
#define AERON_AERON_DRIVER_SENDER_PROXY_H

#include "aeron_driver_context.h"
#include "media/aeron_send_channel_endpoint.h"

void aeron_driver_sender_proxy_register_endpoint(
    aeron_driver_context_t *context, aeron_send_channel_endpoint_t *endpoint);

#endif //AERON_AERON_DRIVER_SENDER_PROXY_H
