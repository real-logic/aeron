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

#ifndef AERON_NETWORK_PUBLICATION_H
#define AERON_NETWORK_PUBLICATION_H

#include "util/aeron_bitutil.h"
#include "util/aeron_fileutil.h"
#include "uri/aeron_uri.h"
#include "aeron_driver_common.h"
#include "aeron_driver_context.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_system_counters.h"
#include "aeron_retransmit_handler.h"

typedef enum aeron_network_publication_state_enum
{
    AERON_NETWORK_PUBLICATION_STATE_ACTIVE,
    AERON_NETWORK_PUBLICATION_STATE_DRAINING,
    AERON_NETWORK_PUBLICATION_STATE_LINGER,
    AERON_NETWORK_PUBLICATION_STATE_CLOSING
}
aeron_network_publication_state_t;

#define AERON_NETWORK_PUBLICATION_HEARTBEAT_TIMEOUT_NS (100 * 1000 * 1000LL)
#define AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS (100 * 1000 * 1000LL)

typedef struct aeron_send_channel_endpoint_stct aeron_send_channel_endpoint_t;
typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

typedef struct aeron_network_publication_stct
{
    struct aeron_network_publication_conductor_fields_stct
    {
        bool has_reached_end_of_life;
        aeron_network_publication_state_t state;
        int32_t refcnt;
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribable_t subscribable;
        int64_t clean_position;
        int64_t time_of_last_activity_ns;
        int64_t last_snd_pos;
    }
    conductor_fields;

    uint8_t conductor_fields_pad[
        (4 * AERON_CACHE_LINE_LENGTH) - sizeof(struct aeron_network_publication_conductor_fields_stct)];

    aeron_mapped_raw_log_t mapped_raw_log;
    aeron_position_t pub_pos_position;
    aeron_position_t pub_lmt_position;
    aeron_position_t snd_pos_position;
    aeron_position_t snd_lmt_position;
    aeron_atomic_counter_t snd_bpe_counter;
    aeron_retransmit_handler_t retransmit_handler;
    aeron_logbuffer_metadata_t *log_meta_data;
    aeron_send_channel_endpoint_t *endpoint;
    aeron_flow_control_strategy_t *flow_control;
    aeron_clock_cache_t *cached_clock;

    char *log_file_name;
    int64_t term_buffer_length;
    int64_t term_window_length;
    int64_t trip_gain;
    int64_t linger_timeout_ns;
    int64_t unblock_timeout_ns;
    int64_t connection_timeout_ns;
    int64_t time_of_last_send_or_heartbeat_ns;
    int64_t time_of_last_setup_ns;
    int64_t status_message_deadline_ns;
    int64_t tag;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    int32_t term_length_mask;
    size_t log_file_name_length;
    size_t position_bits_to_shift;
    size_t mtu_length;
    bool is_exclusive;
    bool spies_simulate_connection;
    bool signal_eos;
    bool should_send_setup_frame;
    bool has_receivers;
    bool has_spies;
    bool is_connected;
    bool is_end_of_stream;
    bool track_sender_limits;
    bool has_sender_released;
    aeron_map_raw_log_close_func_t map_raw_log_close_func;

    int64_t *short_sends_counter;
    int64_t *heartbeats_sent_counter;
    int64_t *sender_flow_control_limits_counter;
    int64_t *retransmits_sent_counter;
    int64_t *unblocked_publications_counter;
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
    aeron_position_t *pub_pos_position,
    aeron_position_t *pub_lmt_position,
    aeron_position_t *snd_pos_position,
    aeron_position_t *snd_lmt_position,
    aeron_atomic_counter_t *snd_bpe_counter,
    aeron_flow_control_strategy_t *flow_control_strategy,
    aeron_uri_publication_params_t *params,
    bool is_exclusive,
    bool spies_simulate_connection,
    aeron_system_counters_t *system_counters);

void aeron_network_publication_close(
    aeron_counters_manager_t *counters_manager, aeron_network_publication_t *publication);

void aeron_network_publication_incref(void *clientd);

void aeron_network_publication_decref(void *clientd);

void aeron_network_publication_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication, int64_t now_ns, int64_t now_ms);

int aeron_network_publication_send(aeron_network_publication_t *publication, int64_t now_ns);
int aeron_network_publication_resend(void *clientd, int32_t term_id, int32_t term_offset, size_t length);

int aeron_network_publication_send_data(
    aeron_network_publication_t *publication, int64_t now_ns, int64_t snd_pos, int32_t term_offset);

void aeron_network_publication_on_nak(
    aeron_network_publication_t *publication, int32_t term_id, int32_t term_offset, int32_t length);

void aeron_network_publication_on_status_message(
    aeron_network_publication_t *publication, const uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_network_publication_on_rttm(
    aeron_network_publication_t *publication, const uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_network_publication_clean_buffer(aeron_network_publication_t *publication, int64_t position);

int aeron_network_publication_update_pub_lmt(aeron_network_publication_t *publication);

void aeron_network_publication_check_for_blocked_publisher(
    aeron_network_publication_t *publication, int64_t now_ns, int64_t producer_position, int64_t snd_pos);

inline void aeron_network_publication_add_subscriber_hook(void *clientd, int64_t *value_addr)
{
    aeron_network_publication_t *publication = (aeron_network_publication_t *)clientd;

    AERON_PUT_ORDERED(publication->has_spies, true);
    if (publication->spies_simulate_connection)
    {
        AERON_PUT_ORDERED(publication->log_meta_data->is_connected, 1);
        AERON_PUT_ORDERED(publication->is_connected, true);
    }
}

inline void aeron_network_publication_remove_subscriber_hook(void *clientd, int64_t *value_addr)
{
    aeron_network_publication_t *publication = (aeron_network_publication_t *)clientd;

    if (1 == publication->conductor_fields.subscribable.length)
    {
        AERON_PUT_ORDERED(publication->has_spies, false);
    }
}

inline bool aeron_network_publication_is_possibly_blocked(
    aeron_network_publication_t *publication, int64_t producer_position, int64_t consumer_position)
{
    int32_t producer_term_count;

    AERON_GET_VOLATILE(producer_term_count, publication->log_meta_data->active_term_count);
    const int32_t expected_term_count = (int32_t)(consumer_position >> publication->position_bits_to_shift);

    if (producer_term_count != expected_term_count)
    {
        return true;
    }

    return producer_position > consumer_position;
}

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

inline int64_t aeron_network_publication_consumer_position(aeron_network_publication_t *publication)
{
    return aeron_counter_get_volatile(publication->snd_pos_position.value_addr);
}

inline void aeron_network_publication_trigger_send_setup_frame(aeron_network_publication_t *publication)
{
    bool is_end_of_stream;
    AERON_GET_VOLATILE(is_end_of_stream, publication->is_end_of_stream);

    if (!is_end_of_stream)
    {
        publication->should_send_setup_frame = true;
    }
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

inline int64_t aeron_network_publication_max_spy_position(aeron_network_publication_t *publication, int64_t snd_pos)
{
    int64_t position = snd_pos;

    for (size_t i = 0, length = publication->conductor_fields.subscribable.length; i < length; i++)
    {
        int64_t spy_position = aeron_counter_get_volatile(
            publication->conductor_fields.subscribable.array[i].value_addr);

        position = spy_position > position ? spy_position : position;
    }

    return position;
}

inline size_t aeron_network_publication_num_spy_subscribers(aeron_network_publication_t *publication)
{
    return publication->conductor_fields.subscribable.length;
}

#endif //AERON_NETWORK_PUBLICATION_H
