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

#ifndef AERON_C_IMAGE_H
#define AERON_C_IMAGE_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_image_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;

    aeron_log_buffer_t *log_buffer;

    int64_t *subscriber_position;

    int64_t correlation_id;
    int32_t session_id;
    int32_t removal_change_number;

    bool is_closed;
}
aeron_image_t;

int aeron_image_create(
    aeron_image_t **image,
    aeron_client_conductor_t *conductor,
    aeron_log_buffer_t *log_buffer,
    int64_t *subscriber_position,
    int64_t correlation_id,
    int32_t session_id);

#endif //AERON_C_IMAGE_H
