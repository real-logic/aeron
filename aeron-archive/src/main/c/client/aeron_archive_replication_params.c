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

int aeron_archive_replication_params_init(aeron_archive_replication_params_t *params)
{
    params->stop_position = AERON_NULL_VALUE;
    params->dst_recording_id = AERON_NULL_VALUE;
    params->live_destination = "";
    params->replication_channel = "";
    params->src_response_channel = "";
    params->channel_tag_id = AERON_NULL_VALUE;
    params->subscription_tag_id = AERON_NULL_VALUE;
    params->file_io_max_length = AERON_NULL_VALUE;
    params->replication_session_id = AERON_NULL_VALUE;
    params->encoded_credentials = NULL;

    return 0;
}
