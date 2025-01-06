/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_CLIENT_H
#define AERON_CLIENT_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_stct
{
    aeron_client_conductor_t conductor;
    aeron_agent_runner_t runner;
    aeron_context_t *context;
}
aeron_t;

int aeron_client_connect_to_driver(aeron_mapped_file_t *cnc_mmap, aeron_context_t *context);

#endif //AERON_CLIENT_H
