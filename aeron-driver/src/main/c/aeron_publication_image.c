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

#include "util/aeron_error.h"
#include "aeron_publication_image.h"

int aeron_publication_image_create(
    aeron_publication_image_t **image,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_driver_context_t *context,
    int64_t correlation_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t initial_term_id,
    int32_t active_term_id,
    int32_t initial_term_offset,
    aeron_position_t *rcv_hwm_position,
    aeron_position_t *rcv_pos_position,
    aeron_congestion_control_strategy_t *congestion_control,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *source_address,
    int32_t term_buffer_length,
    int32_t sender_mtu_length,
    bool is_reliable,
    aeron_system_counters_t *system_counters)
{
    char path[AERON_MAX_PATH];
    int path_length =
        aeron_publication_image_location(
            path,
            sizeof(path),
            context->aeron_dir,
            endpoint->conductor_fields.udp_channel->canonical_form,
            session_id,
            stream_id,
            correlation_id);
    aeron_publication_image_t *_image = NULL;
    const uint64_t usable_fs_space = context->usable_fs_space_func(context->aeron_dir);
    const uint64_t log_length = AERON_LOGBUFFER_COMPUTE_LOG_LENGTH(term_buffer_length);

    *image = NULL;

    if (usable_fs_space < log_length)
    {
        aeron_set_err(ENOSPC, "Insufficient usable storage for new log of length=%d in %s", log_length, context->aeron_dir);
        return -1;
    }

    if (aeron_alloc((void **)&_image, sizeof(aeron_publication_image_t)) < 0)
    {
        aeron_set_err(ENOMEM, "%s", "Could not allocate publication image");
        return -1;
    }

    _image->log_file_name = NULL;
    if (aeron_alloc((void **)(&_image->log_file_name), (size_t)path_length) < 0)
    {
        aeron_free(_image);
        aeron_set_err(ENOMEM, "%s", "Could not allocate publication image log_file_name");
        return -1;
    }

    if (context->map_raw_log_func(&_image->mapped_raw_log, path, context->term_buffer_sparse_file, (uint64_t)term_buffer_length) < 0)
    {
        aeron_free(_image->log_file_name);
        aeron_free(_image);
        aeron_set_err(aeron_errcode(), "error mapping network raw log %s: %s", path, aeron_errmsg());
        return -1;
    }
    _image->map_raw_log_close_func = context->map_raw_log_close_func;

    strncpy(_image->log_file_name, path, path_length);
    _image->log_file_name_length = (size_t)path_length;
    _image->log_meta_data = (aeron_logbuffer_metadata_t *)(_image->mapped_raw_log.log_meta_data.addr);

    _image->log_meta_data->initialTerm_id = initial_term_id;
    _image->log_meta_data->mtu_length = sender_mtu_length;
    _image->log_meta_data->correlation_id = correlation_id;
    _image->log_meta_data->time_of_last_status_message = 0;
    _image->log_meta_data->end_of_stream_position = INT64_MAX;
    aeron_logbuffer_fill_default_header(
        _image->mapped_raw_log.log_meta_data.addr, session_id, stream_id, initial_term_id);

    _image->endpoint = endpoint;
    _image->congestion_control = congestion_control;
    _image->nano_clock = context->nano_clock;
    _image->epoch_clock = context->epoch_clock;
    _image->conductor_fields.subscribeable.array = NULL;
    _image->conductor_fields.subscribeable.length = 0;
    _image->conductor_fields.subscribeable.capacity = 0;
    _image->conductor_fields.managed_resource.registration_id = correlation_id;
    _image->conductor_fields.managed_resource.clientd = _image;
    _image->conductor_fields.managed_resource.incref = NULL;
    _image->conductor_fields.managed_resource.decref = NULL;
    _image->conductor_fields.has_reached_end_of_life = false;
    _image->conductor_fields.status = AERON_PUBLICATION_IMAGE_STATUS_INIT;
    _image->conductor_fields.time_of_last_activity_ns = 0;
    _image->conductor_fields.liveness_timeout_ns = context->image_liveness_timeout_ns;
    _image->session_id = session_id;
    _image->stream_id = stream_id;
    _image->rcv_hwm_position.counter_id = rcv_hwm_position->counter_id;
    _image->rcv_hwm_position.value_addr = rcv_hwm_position->value_addr;
    _image->rcv_pos_position.counter_id = rcv_pos_position->counter_id;
    _image->rcv_pos_position.value_addr = rcv_pos_position->value_addr;
    _image->initial_term_id = initial_term_id;
    _image->term_length_mask = (int32_t)term_buffer_length - 1;
    _image->position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes((int32_t)term_buffer_length);
    _image->mtu_length = sender_mtu_length;

    memcpy(&_image->control_address, control_address, sizeof(_image->control_address));
    memcpy(&_image->source_address, source_address, sizeof(_image->source_address));

    _image->heartbeats_received_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_HEARTBEATS_RECEIVED);
    _image->flow_control_under_runs_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_FLOW_CONTROL_UNDER_RUNS);
    _image->flow_control_over_runs_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_FLOW_CONTROL_OVER_RUNS);

    const int64_t initial_position =
        aeron_logbuffer_compute_position(
            active_term_id, initial_term_offset, _image->position_bits_to_shift, initial_term_id);

    _image->next_sm_position = initial_position;
    _image->next_sm_receiver_window_length =
        _image->congestion_control->initial_window_length(_image->congestion_control->state);
    _image->conductor_fields.clean_position = initial_position;

    aeron_counter_set_ordered(_image->rcv_hwm_position.value_addr, initial_position);
    aeron_counter_set_ordered(_image->rcv_pos_position.value_addr, initial_position);

    *image = _image;
    return 0;
}

int aeron_publication_image_close(aeron_counters_manager_t *counters_manager, aeron_publication_image_t *image)
{
    if (NULL != image)
    {
        aeron_subscribeable_t *subscribeable = &image->conductor_fields.subscribeable;

        aeron_counters_manager_free(counters_manager, (int32_t)image->rcv_hwm_position.counter_id);
        aeron_counters_manager_free(counters_manager, (int32_t)image->rcv_pos_position.counter_id);

        for (size_t i = 0, length = subscribeable->length; i < length; i++)
        {
            aeron_counters_manager_free(counters_manager, (int32_t)subscribeable->array[i].counter_id);
        }

        aeron_free(subscribeable->array);

        image->map_raw_log_close_func(&image->mapped_raw_log);
        image->congestion_control->fini(image->congestion_control);
        aeron_free(image->log_file_name);
    }

    aeron_free(image);
    return 0;
}

void aeron_publication_image_track_rebuild(
    aeron_publication_image_t *image, int64_t now_nw, int64_t status_message_timeout)
{

}

int aeron_publication_image_insert_packet(
    aeron_publication_image_t *image, int32_t term_id, int32_t term_offset, const uint8_t *buffer, size_t length)
{
    const bool is_heartbeat = aeron_publication_image_is_heartbeat(buffer, length);
    const int64_t packet_position =
        aeron_logbuffer_compute_position(term_id, term_offset, image->position_bits_to_shift, image->initial_term_id);
    const int64_t proposed_position = is_heartbeat ? packet_position : packet_position + (int64_t)length;
    const int64_t window_position = image->next_sm_position;

    if (!aeron_publication_image_is_flow_control_under_run(image, window_position, packet_position) &&
        !aeron_publication_image_is_flow_control_over_run(image, window_position, proposed_position))
    {
        if (aeron_publication_image_is_heartbeat(buffer, length))
        {
            if (aeron_publication_image_is_end_of_stream(buffer, length))
            {
                AERON_PUT_ORDERED(image->log_meta_data->end_of_stream_position, packet_position);
            }

            aeron_counter_ordered_increment(image->heartbeats_received_counter, 1);
        }
        else
        {
            /* TODO: TermRebuilder.insert */
        }

        aeron_publication_image_hwm_candidate(image, proposed_position);
    }

    return (int)length;
}

int aeron_publication_image_on_rttm(
    aeron_publication_image_t *image, aeron_rttm_header_t *header, struct sockaddr_storage *addr)
{
    const int64_t now_ns = image->nano_clock();
    const int64_t rtt_in_ns = now_ns - header->echo_timestamp - header->reception_delta;

    image->congestion_control->on_rttm(image->congestion_control->state, now_ns, rtt_in_ns, addr);
    return 1;
}

extern bool aeron_publication_image_is_heartbeat(const uint8_t *buffer, size_t length);
extern bool aeron_publication_image_is_end_of_stream(const uint8_t *buffer, size_t length);
extern bool aeron_publication_image_is_flow_control_under_run(
    aeron_publication_image_t *image, int64_t window_position, int64_t packet_position);
extern bool aeron_publication_image_is_flow_control_over_run(
    aeron_publication_image_t *image, int64_t window_position, int64_t proposed_position);
extern void aeron_publication_image_hwm_candidate(aeron_publication_image_t *image, int64_t proposed_position);
