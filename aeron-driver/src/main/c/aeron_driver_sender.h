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

#ifndef AERON_AERON_DRIVER_SENDER_H
#define AERON_AERON_DRIVER_SENDER_H

#include "aeron_driver_context.h"
#include "aeron_driver_sender_proxy.h"
#include "aeron_system_counters.h"

typedef struct aeron_driver_sender_stct
{
    aeron_driver_sender_proxy_t sender_proxy;
    aeron_driver_context_t *context;
}
aeron_driver_sender_t;

int aeron_driver_sender_init(
    aeron_driver_sender_t *sender, aeron_driver_context_t *context, aeron_system_counters_t *system_counters);

int aeron_driver_sender_do_work(void *clientd);
void aeron_driver_sender_on_close(void *clientd);

void aeron_driver_sender_on_add_endpoint(void *clientd, void *command);
void aeron_driver_sender_on_remove_endpoint(void *clientd, void *command);

#endif //AERON_AERON_DRIVER_SENDER_H
