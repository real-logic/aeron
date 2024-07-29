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

#include "aeron_archive.h"
#include "aeron_archive_replay_params.h"

int aeron_archive_replay_params_init(aeron_archive_replay_params_t *params)
{
    params->bounding_limit_counter_id = AERON_NULL_VALUE;
    params->file_io_max_length = AERON_NULL_VALUE;
    params->position = AERON_NULL_VALUE;
    params->length = AERON_NULL_VALUE;
    params->replay_token = AERON_NULL_VALUE;
    params->subscription_registration_id = AERON_NULL_VALUE;

    return 0;
}

bool aeron_archive_replay_params_is_bounded(aeron_archive_replay_params_t *params)
{
    return AERON_NULL_COUNTER_ID != params->bounding_limit_counter_id;
}