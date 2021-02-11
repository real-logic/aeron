/*
 * Copyright 2014-2021 Real Logic Limited.
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

#ifndef AERON_DRIVER_AGENT_H
#define AERON_DRIVER_AGENT_H

#include "aeron_driver_conductor.h"
#include "command/aeron_control_protocol.h"

#define AERON_EVENT_LOG_ENV_VAR "AERON_EVENT_LOG"
#define AERON_EVENT_LOG_DISABLE_ENV_VAR "AERON_EVENT_LOG_DISABLE"
#define AERON_EVENT_LOG_FILENAME_ENV_VAR "AERON_EVENT_LOG_FILENAME"
#define AERON_EVENT_RB_LENGTH (8 * 1024 * 1024)
#define AERON_MAX_CMD_LENGTH (512)
#define AERON_MAX_FRAME_LENGTH (1408)

#define AERON_DRIVER_AGENT_LOG_CONTEXT "DRIVER"
#define AERON_DRIVER_AGENT_ALL_EVENTS "all"
#define AERON_DRIVER_AGENT_ADMIN_EVENTS "admin"
#define AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME "unknown"

typedef enum aeron_driver_agent_event_enum
{
    AERON_DRIVER_EVENT_FRAME_IN = 1,
    AERON_DRIVER_EVENT_FRAME_OUT = 2,
    AERON_DRIVER_EVENT_CMD_IN_ADD_PUBLICATION = 3,
    AERON_DRIVER_EVENT_CMD_IN_REMOVE_PUBLICATION = 4,
    AERON_DRIVER_EVENT_CMD_IN_ADD_SUBSCRIPTION = 5,
    AERON_DRIVER_EVENT_CMD_IN_REMOVE_SUBSCRIPTION = 6,
    AERON_DRIVER_EVENT_CMD_OUT_PUBLICATION_READY = 7,
    AERON_DRIVER_EVENT_CMD_OUT_AVAILABLE_IMAGE = 8,
    AERON_DRIVER_EVENT_CMD_OUT_ON_OPERATION_SUCCESS = 12,
    AERON_DRIVER_EVENT_CMD_IN_KEEPALIVE_CLIENT = 13,
    AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP = 14,
    AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP = 15,
    AERON_DRIVER_EVENT_REMOVE_IMAGE_CLEANUP = 16,
    AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_IMAGE = 17,
    AERON_DRIVER_EVENT_SEND_CHANNEL_CREATION = 23,
    AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CREATION = 24,
    AERON_DRIVER_EVENT_SEND_CHANNEL_CLOSE = 25,
    AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CLOSE = 26,
    AERON_DRIVER_EVENT_CMD_IN_ADD_DESTINATION = 30,
    AERON_DRIVER_EVENT_CMD_IN_REMOVE_DESTINATION = 31,
    AERON_DRIVER_EVENT_CMD_IN_ADD_EXCLUSIVE_PUBLICATION = 32,
    AERON_DRIVER_EVENT_CMD_OUT_EXCLUSIVE_PUBLICATION_READY = 33,
    AERON_DRIVER_EVENT_CMD_OUT_ERROR = 34,
    AERON_DRIVER_EVENT_CMD_IN_ADD_COUNTER = 35,
    AERON_DRIVER_EVENT_CMD_IN_REMOVE_COUNTER = 36,
    AERON_DRIVER_EVENT_CMD_OUT_SUBSCRIPTION_READY = 37,
    AERON_DRIVER_EVENT_CMD_OUT_COUNTER_READY = 38,
    AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_COUNTER = 39,
    AERON_DRIVER_EVENT_CMD_IN_CLIENT_CLOSE = 40,
    AERON_DRIVER_EVENT_CMD_IN_ADD_RCV_DESTINATION = 41,
    AERON_DRIVER_EVENT_CMD_IN_REMOVE_RCV_DESTINATION = 42,
    AERON_DRIVER_EVENT_CMD_OUT_ON_CLIENT_TIMEOUT = 43,
    AERON_DRIVER_EVENT_CMD_IN_TERMINATE_DRIVER = 44,
    AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE = 45,
    AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED = 46,
    AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_REMOVED = 47,

    // C-specific events. Note: event IDs are dynamic to avoid gaps in the sparse arrays.
    AERON_DRIVER_EVENT_ADD_DYNAMIC_DISSECTOR,
    AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT,

    AERON_DRIVER_EVENT_NUM_ELEMENTS, // number of elements in this enum (including gaps)
    AERON_DRIVER_EVENT_UNKNOWN_EVENT = -1
}
aeron_driver_agent_event_t;

typedef struct aeron_driver_agent_log_header_stct
{
    int64_t time_ns;
}
aeron_driver_agent_log_header_t;

typedef struct aeron_driver_agent_cmd_log_header_stct
{
    int64_t time_ns;
    int64_t cmd_id;
}
aeron_driver_agent_cmd_log_header_t;

typedef struct aeron_driver_agent_frame_log_header_stct
{
    int64_t time_ns;
    int32_t result;
    int32_t sockaddr_len;
    int32_t message_len;
}
aeron_driver_agent_frame_log_header_t;

typedef struct aeron_driver_agent_untethered_subscription_state_change_log_header_stct
{
    int64_t time_ns;
    int64_t subscription_id;
    int32_t stream_id;
    int32_t session_id;
    aeron_subscription_tether_state_t old_state;
    aeron_subscription_tether_state_t new_state;
}
aeron_driver_agent_untethered_subscription_state_change_log_header_t;

typedef struct aeron_driver_agent_remove_resource_cleanup_stct
{
    int64_t time_ns;
    int64_t id;
    int32_t stream_id;
    int32_t session_id;
    int32_t channel_length;
}
aeron_driver_agent_remove_resource_cleanup_t;

typedef struct aeron_driver_agent_on_endpoint_change_stct
{
    int64_t time_ns;
    struct sockaddr_storage local_data;
    struct sockaddr_storage remote_data;
    uint8_t multicast_ttl;
}
aeron_driver_agent_on_endpoint_change_t;

typedef void (*aeron_driver_agent_generic_dissector_func_t)(
    FILE *fpout, const char *log_header_str, const void *message, size_t len);

typedef struct aeron_driver_agent_add_dissector_header_stct
{
    int64_t time_ns;
    int64_t index;
    aeron_driver_agent_generic_dissector_func_t dissector_func;
}
aeron_driver_agent_add_dissector_header_t;

typedef struct aeron_driver_agent_dynamic_event_header_stct
{
    int64_t time_ns;
    int64_t index;
}
aeron_driver_agent_dynamic_event_header_t;

aeron_mpsc_rb_t *aeron_driver_agent_mpsc_rb();

typedef int (*aeron_driver_context_init_t)(aeron_driver_context_t **);

int aeron_driver_agent_context_init(aeron_driver_context_t *context);

const char *aeron_driver_agent_dissect_log_header(
    int64_t time_ns,
    aeron_driver_agent_event_t event_id,
    size_t capture_length,
    size_t message_length);

const char *aeron_driver_agent_dissect_log_start(int64_t time_ns, int64_t time_ms);

void aeron_driver_agent_log_dissector(int32_t msg_type_id, const void *message, size_t length, void *clientd);

int aeron_driver_agent_init_logging_events_interceptors(aeron_driver_context_t *context);

void aeron_driver_agent_logging_ring_buffer_init();

void aeron_driver_agent_logging_ring_buffer_free();

bool aeron_driver_agent_logging_events_init(const char *event_log, const char *event_log_disable);

void aeron_driver_agent_logging_events_free();

bool aeron_driver_agent_is_event_enabled(aeron_driver_agent_event_t id);

const char *aeron_driver_agent_event_name(aeron_driver_agent_event_t id);

void aeron_driver_agent_untethered_subscription_state_change_interceptor(
    aeron_tetherable_position_t *tetherable_position,
    int64_t now_ns,
    aeron_subscription_tether_state_t new_state,
    int32_t stream_id,
    int32_t session_id);

void aeron_driver_agent_name_resolution_on_neighbor_added(const struct sockaddr_storage *addr);

void aeron_driver_agent_name_resolution_on_neighbor_removed(const struct sockaddr_storage *addr);

void aeron_driver_agent_remove_publication_cleanup(
    int32_t session_id, int32_t stream_id, size_t channel_length, const char *channel);

void aeron_driver_agent_remove_subscription_cleanup(
    int64_t id, int32_t stream_id, size_t channel_length, const char *channel);

void aeron_driver_agent_remove_image_cleanup(
    int64_t id, int32_t session_id, int32_t stream_id, size_t channel_length, const char *channel);

void aeron_driver_agent_conductor_to_driver_interceptor(
    int32_t msg_type_id, const void *message, size_t length, void *clientd);

void aeron_driver_agent_conductor_to_client_interceptor(
    aeron_driver_conductor_t *conductor, int32_t msg_type_id, const void *message, size_t length);

void aeron_driver_agent_log_frame(
    int32_t msg_type_id, const struct msghdr *msghdr, int result, int32_t message_len);

void aeron_driver_agent_sender_proxy_on_add_endpoint(const void *channel);

void aeron_driver_agent_sender_proxy_on_remove_endpoint(const void *channel);

void aeron_driver_agent_receiver_proxy_on_add_endpoint(const void *channel);

void aeron_driver_agent_receiver_proxy_on_remove_endpoint(const void *channel);

int64_t aeron_driver_agent_add_dynamic_dissector(aeron_driver_agent_generic_dissector_func_t func);

void aeron_driver_agent_log_dynamic_event(int64_t index, const void *message, size_t length);

#endif //AERON_DRIVER_AGENT_H
