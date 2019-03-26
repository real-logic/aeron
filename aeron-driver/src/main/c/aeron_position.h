/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#ifndef AERON_DRIVER_POSITION_H
#define AERON_DRIVER_POSITION_H

#include "concurrent/aeron_counters_manager.h"

int32_t aeron_stream_counter_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel,
    const char *suffix);

int32_t aeron_channel_endpoint_status_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int32_t channel_length,
    const char *channel);

int32_t aeron_heartbeat_status_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id);

#define AERON_COUNTER_PUBLISHER_LIMIT_NAME "pub-lmt"
#define AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID (1)

#define AERON_COUNTER_SENDER_POSITION_NAME "snd-pos"
#define AERON_COUNTER_SENDER_POSITION_TYPE_ID (2)

#define AERON_COUNTER_SENDER_LIMIT_NAME "snd-lmt"
#define AERON_COUNTER_SENDER_LIMIT_TYPE_ID (9)

int32_t aeron_counter_publisher_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel);

int32_t aeron_counter_sender_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel);

int32_t aeron_counter_sender_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel);

#define AERON_COUNTER_SUBSCRIPTION_POSITION_NAME "sub-pos"
#define AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID (4)

int32_t aeron_counter_subscription_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel,
    int64_t joining_position);

#define AERON_COUNTER_RECEIVER_HWM_NAME "rcv-hwm"
#define AERON_COUNTER_RECEIVER_HWM_TYPE_ID (3)

int32_t aeron_counter_receiver_hwm_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel);

#define AERON_COUNTER_RECEIVER_POSITION_NAME "rcv-pos"
#define AERON_COUNTER_RECEIVER_POSITION_TYPE_ID (5)

int32_t aeron_counter_receiver_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel);

#define AERON_COUNTER_SEND_CHANNEL_STATUS_NAME "snd-channel"
#define AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID (6)

#define AERON_COUNTER_RECEIVE_CHANNEL_STATUS_NAME "rcv-channel"
#define AERON_COUNTER_RECEIVE_CHANNEL_STATUS_TYPE_ID (7)

#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_INITIALIZING (0)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ERRORED (-1)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE (1)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_CLOSING (2)

int32_t aeron_counter_send_channel_status_allocate(
    aeron_counters_manager_t *counters_manager,
    int32_t channel_length,
    const char *channel);

int32_t aeron_counter_receive_channel_status_allocate(
    aeron_counters_manager_t *counters_manager,
    int32_t channel_length,
    const char *channel);

#define AERON_COUNTER_CLIENT_HEARTBEAT_STATUS_NAME "client-heartbeat"
#define AERON_COUNTER_CLIENT_HEARTBEAT_STATUS_TYPE_ID (11)

int32_t aeron_counter_client_heartbeat_status_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t client_id);

#define AERON_COUNTER_PUBLISHER_POSITION_NAME "pub-pos"
#define AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID (12)

int32_t aeron_counter_publisher_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel);

#define AERON_COUNTER_SENDER_BPE_NAME "snd-bpe"
#define AERON_COUNTER_SENDER_BPE_TYPE_ID  (13)

int32_t aeron_counter_sender_bpe_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel);

#endif
