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
#include "aeron_common.h"

#include <stdio.h>

typedef struct aeron_archive_stct aeron_archive_t;
typedef struct aeron_archive_context_stct aeron_archive_context_t;
typedef struct aeron_archive_async_connect_stct aeron_archive_async_connect_t;
typedef struct aeron_archive_control_response_poller_stct aeron_archive_control_response_poller_t;

typedef enum aeron_archive_source_location_en
{
    AERON_ARCHIVE_SOURCE_LOCATION_LOCAL = 0,
    AERON_ARCHIVE_SOURCE_LOCATION_REMOTE = 1
}
aeron_archive_source_location_t;

int aeron_archive_context_init(aeron_archive_context_t **ctx);
int aeron_archive_context_close(aeron_archive_context_t *ctx);

int aeron_archive_async_connect(aeron_archive_async_connect_t **async, aeron_archive_context_t *ctx);
int aeron_archive_async_connect_poll(aeron_archive_t **aeron_archive, aeron_archive_async_connect_t *async);

int aeron_archive_connect(aeron_archive_t **aeron_archive, aeron_archive_context_t *ctx);

int aeron_archive_start_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

int aeron_archive_get_recording_position(
    int64_t *recording_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

int aeron_archive_get_stop_position(
    int64_t *stop_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

int aeron_archive_get_max_recorded_position(
    int64_t *max_recorded_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

int aeron_archive_stop_recording(
    aeron_archive_t *aeron_archive,
    int64_t subscription_id,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

int aeron_archive_find_last_matching_recording(
    int64_t *recording_id_p,
    aeron_archive_t *aeron_archive,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

aeron_t *aeron_archive_get_aeron(aeron_archive_t *aeron_archive);

int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, int64_t message_timeout_ns);

/* maybe these can be in the 'private' headers because they'll only be used for unit tests? */
int64_t aeron_archive_get_archive_id(aeron_archive_t *aeron_archive);
aeron_archive_control_response_poller_t *aeron_archive_get_control_response_poller(aeron_archive_t *aeron_archive);

aeron_subscription_t *aeron_archive_control_response_poller_get_subscription(aeron_archive_control_response_poller_t *poller);

int32_t aeron_archive_recording_pos_find_counter_id_by_session_id(aeron_counters_reader_t *counters_reader, int32_t session_id);
int64_t aeron_archive_recording_pos_get_recording_id(aeron_counters_reader_t *counters_reader, int32_t counter_id);

#endif //AERON_ARCHIVE_H
