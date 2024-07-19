/*
 * Copyright 2014-2024 Real Logic Limited.
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

#ifndef AERON_ARCHIVE_H
#define AERON_ARCHIVE_H

#include "aeronc.h"

#include <stdio.h>

typedef struct aeron_archive_stct aeron_archive_t;
typedef struct aeron_archive_context_stct aeron_archive_context_t;
typedef struct aeron_archive_async_connect_stct aeron_archive_async_connect_t;
typedef struct aeron_archive_control_response_poller_stct aeron_archive_control_response_poller_t;

int aeron_archive_context_init(aeron_archive_context_t **ctx);
int aeron_archive_context_close(aeron_archive_context_t *ctx);

int aeron_archive_async_connect(aeron_archive_async_connect_t **async, aeron_archive_context_t *ctx);
int aeron_archive_async_connect_poll(aeron_archive_t **client, aeron_archive_async_connect_t *async);

int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, int64_t message_timeout_ns);

/* maybe these can be in the 'private' headers because they'll only be used for unit tests? */
int64_t aeron_archive_get_archive_id(aeron_archive_t *client);
aeron_archive_control_response_poller_t *aeron_archive_get_control_response_poller(aeron_archive_t *client);

aeron_subscription_t *aeron_archive_control_response_poller_get_subscription(aeron_archive_control_response_poller_t *control_response_poller);

#endif //AERON_ARCHIVE_H
