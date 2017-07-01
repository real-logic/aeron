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

#ifndef AERON_AERON_NETWORK_PUBLICATION_H
#define AERON_AERON_NETWORK_PUBLICATION_H

#include "util/aeron_bitutil.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver_common.h"
#include "aeron_driver_context.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_system_counters.h"
#include "aeron_retransmit_handler.h"

typedef enum aeron_network_publication_status_enum
{
    AERON_NETWORK_PUBLICATION_STATUS_ACTIVE,
    AERON_NETWORK_PUBLICATION_STATUS_DRAINING,
    AERON_NETWORK_PUBLICATION_STATUS_LINGER,
    AERON_NETWORK_PUBLICATION_STATUS_CLOSING
}
aeron_network_publication_status_t;

#define AERON_NETWORK_PUBLICATION_HEARTBEAT_TIMEOUT_NS (100 * 1000 * 1000L)
#define AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS (100 * 1000 * 1000L)
#define AERON_NETWORK_PUBLICATION_CONNECTION_TIMEOUT_MS (5 * 1000L)

#define AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND (2)

typedef struct aeron_send_channel_endpoint_stct aeron_send_channel_endpoint_t;
typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

typedef struct aeron_network_publication_stct
{
    struct aeron_network_publication_conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribeable_t subscribeable;
        int64_t clean_position;
        int64_t time_of_last_activity_ns;
        int64_t last_snd_pos;
        int32_t refcnt;
        bool has_reached_end_of_life;
        aeron_network_publication_status_t status;
    }
    conductor_fields;

    uint8_t conductor_fields_pad[
        (2 * AERON_CACHE_LINE_LENGTH) - sizeof(struct aeron_network_publication_conductor_fields_stct)];

    aeron_mapped_raw_log_t mapped_raw_log;
    aeron_position_t pub_lmt_position;
    aeron_position_t snd_pos_position;
    aeron_position_t snd_lmt_position;
    aeron_retransmit_handler_t retransmit_handler;
    aeron_logbuffer_metadata_t *log_meta_data;
    aeron_send_channel_endpoint_t *endpoint;
    aeron_flow_control_strategy_t *flow_control;
    aeron_clock_func_t epoch_clock;
    aeron_clock_func_t nano_clock;

    char *log_file_name;
    int64_t term_window_length;
    int64_t trip_gain;
    int64_t linger_timeout_ns;
    int64_t time_of_last_send_or_heartbeat_ns;
    int64_t time_of_last_setup_ns;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    int32_t term_length_mask;
    size_t log_file_name_length;
    size_t position_bits_to_shift;
    size_t mtu_length;
    bool is_exclusive;
    bool should_send_setup_frame;
    bool is_connected;
    bool is_complete;
    bool track_sender_limits;
    bool has_sender_released;
    aeron_map_raw_log_close_func_t map_raw_log_close_func;

    int64_t *short_sends_counter;
    int64_t *heartbeats_sent_counter;
    int64_t *sender_flow_control_limits_counter;
    int64_t *retransmits_sent_counter;
}
aeron_network_publication_t;

int aeron_network_publication_create(
    aeron_network_publication_t **publication,
    aeron_send_channel_endpoint_t *endpoint,
    aeron_driver_context_t *context,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t initial_term_id,
    size_t mtu_length,
    aeron_position_t *pub_lmt_position,
    aeron_position_t *snd_pos_position,
    aeron_position_t *snd_lmt_position,
    aeron_flow_control_strategy_t *flow_control_strategy,
    size_t term_buffer_length,
    bool is_exclusive,
    aeron_system_counters_t *system_counters);

void aeron_network_publication_close(aeron_counters_manager_t *counters_manager, aeron_network_publication_t *publication);

void aeron_network_publication_incref(void *clientd);
void aeron_network_publication_decref(void *clientd);
void aeron_network_publication_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication, int64_t now_ns, int64_t now_ms);

int aeron_network_publication_send(aeron_network_publication_t *publication, int64_t now_ns);

int aeron_network_publication_send_data(
    aeron_network_publication_t *publication, int64_t now_ns, int64_t snd_pos, int32_t term_offset);

void aeron_network_publication_on_nak(
    aeron_network_publication_t *publication, int32_t term_id, int32_t term_offset, int32_t length);

void aeron_network_publication_on_status_message(
    aeron_network_publication_t *publication, const uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_network_publication_on_rttm(
    aeron_network_publication_t *publication, const uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_network_publication_clean_buffer(aeron_network_publication_t *publication, int64_t pub_lmt);

int aeron_network_publication_update_pub_lmt(aeron_network_publication_t *publication);

void aeron_network_publication_check_for_blocked_publisher(
    aeron_network_publication_t *publication, int64_t now_ns, int64_t snd_pos);

inline int64_t aeron_network_publication_producer_position(aeron_network_publication_t *publication)
{
    int64_t raw_tail;

    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, publication->log_meta_data);

    return aeron_logbuffer_compute_position(
        aeron_logbuffer_term_id(raw_tail),
        aeron_logbuffer_term_offset(raw_tail, (int32_t)publication->mapped_raw_log.term_length),
        publication->position_bits_to_shift,
        publication->initial_term_id);
}

inline int64_t aeron_network_publication_spy_join_position(aeron_network_publication_t *publication)
{
    int64_t max_spy_position = aeron_network_publication_producer_position(publication);

    for (size_t i = 0, length = publication->conductor_fields.subscribeable.length; i < length; i++)
    {
        int64_t position = aeron_counter_get_volatile(publication->conductor_fields.subscribeable.array[i].value_addr);

        max_spy_position = position > max_spy_position ? position : max_spy_position;
    }

    return max_spy_position;
}

inline void aeron_network_publication_trigger_send_setup_frame(aeron_network_publication_t *publication)
{
    publication->should_send_setup_frame = true;
}

inline void aeron_network_publication_sender_release(aeron_network_publication_t *publication)
{
    AERON_PUT_ORDERED(publication->has_sender_released, true);
}

inline bool aeron_network_publication_has_sender_released(aeron_network_publication_t *publication)
{
    bool has_sender_released;
    AERON_GET_VOLATILE(has_sender_released, publication->has_sender_released);

    return has_sender_released;
}

#endif //AERON_AERON_NETWORK_PUBLICATION_H
