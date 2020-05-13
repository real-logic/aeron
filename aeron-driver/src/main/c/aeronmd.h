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

#ifndef AERON_AERONMD_H
#define AERON_AERONMD_H

#ifdef __cplusplus
extern "C"
{
#endif

#include <stdbool.h>
#include <stdint.h>

typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_driver_stct aeron_driver_t;

/**
 * Environment variables used for setting values of an aeron_driver_context_t.
 */

/**
 * The top level Aeron directory used for communication between a Media Driver and client.
 */
#define AERON_DIR_ENV_VAR "AERON_DIR"

int aeron_driver_context_set_dir(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_dir(aeron_driver_context_t *context);

/**
 * Warn if the top level Aeron directory exists when starting the driver.
 */
#define AERON_DIR_WARN_IF_EXISTS_ENV_VAR "AERON_DIR_WARN_IF_EXISTS"

int aeron_driver_context_set_dir_warn_if_exists(aeron_driver_context_t *context, bool value);
bool aeron_driver_context_get_dir_warn_if_exists(aeron_driver_context_t *context);

/**
 * Threading Mode to be used by the driver.
 */
#define AERON_THREADING_MODE_ENV_VAR "AERON_THREADING_MODE"

typedef enum aeron_threading_mode_enum
{
    AERON_THREADING_MODE_DEDICATED,
    AERON_THREADING_MODE_SHARED_NETWORK,
    AERON_THREADING_MODE_SHARED,
    AERON_THREADING_MODE_INVOKER
}
aeron_threading_mode_t;

int aeron_driver_context_set_threading_mode(aeron_driver_context_t *context, aeron_threading_mode_t mode);
aeron_threading_mode_t aeron_driver_context_get_threading_mode(aeron_driver_context_t *context);

/**
 * Attempt to delete directories on start if they exist.
 */
#define AERON_DIR_DELETE_ON_START_ENV_VAR "AERON_DIR_DELETE_ON_START"

int aeron_driver_context_set_dir_delete_on_start(aeron_driver_context_t * context, bool value);
bool aeron_driver_context_get_dir_delete_on_start(aeron_driver_context_t *context);

/**
 * Attempt to delete directories on shutdown.
 */
#define AERON_DIR_DELETE_ON_SHUTDOWN_ENV_VAR "AERON_DIR_DELETE_ON_SHUTDOWN"

int aeron_driver_context_set_dir_delete_on_shutdown(aeron_driver_context_t * context, bool value);
bool aeron_driver_context_get_dir_delete_on_shutdown(aeron_driver_context_t *context);

/**
 * Length (in bytes) of the conductor buffer for control commands from the clients to the media driver conductor.
 */
#define AERON_TO_CONDUCTOR_BUFFER_LENGTH_ENV_VAR "AERON_CONDUCTOR_BUFFER_LENGTH"

int aeron_driver_context_set_to_conductor_buffer_length(aeron_driver_context_t *context, size_t length);
size_t aeron_driver_context_get_to_conductor_buffer_length(aeron_driver_context_t *context);

/**
 * Length (in bytes) of the broadcast buffers from the media driver to the clients.
 */
#define AERON_TO_CLIENTS_BUFFER_LENGTH_ENV_VAR "AERON_CLIENTS_BUFFER_LENGTH"

int aeron_driver_context_set_to_clients_buffer_length(aeron_driver_context_t *context, size_t length);
size_t aeron_driver_context_get_to_clients_buffer_length(aeron_driver_context_t *context);

/**
 * Length (in bytes) of the value buffer for the system counters.
 */
#define AERON_COUNTERS_VALUES_BUFFER_LENGTH_ENV_VAR "AERON_COUNTERS_BUFFER_LENGTH"

int aeron_driver_context_set_counters_buffer_length(aeron_driver_context_t *context, size_t length);
size_t aeron_driver_context_get_counters_buffer_length(aeron_driver_context_t *context);

/**
 * Length (in bytes) of the buffer for the distinct error log.
 */
#define AERON_ERROR_BUFFER_LENGTH_ENV_VAR "AERON_ERROR_BUFFER_LENGTH"

int aeron_driver_context_set_error_buffer_length(aeron_driver_context_t *context, size_t length);
size_t aeron_driver_context_get_error_buffer_length(aeron_driver_context_t *context);

/**
 * Client liveness timeout in nanoseconds
 */
#define AERON_CLIENT_LIVENESS_TIMEOUT_ENV_VAR "AERON_CLIENT_LIVENESS_TIMEOUT"

int aeron_driver_context_set_client_liveness_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_client_liveness_timeout_ns(aeron_driver_context_t *context);

/**
 * Length (in bytes) of the log buffers for publication terms.
 */
#define AERON_TERM_BUFFER_LENGTH_ENV_VAR "AERON_TERM_BUFFER_LENGTH"

int aeron_driver_context_set_term_buffer_length(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_term_buffer_length(aeron_driver_context_t *context);

/**
 * Length (in bytes) of the log buffers for IPC publication terms.
 */
#define AERON_IPC_TERM_BUFFER_LENGTH_ENV_VAR "AERON_IPC_TERM_BUFFER_LENGTH"

int aeron_driver_context_set_ipc_term_buffer_length(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_ipc_term_buffer_length(aeron_driver_context_t *context);

/**
 * Should term buffers be created sparse.
 */
#define AERON_TERM_BUFFER_SPARSE_FILE_ENV_VAR "AERON_TERM_BUFFER_SPARSE_FILE"

int aeron_driver_context_set_term_buffer_sparse_file(aeron_driver_context_t *context, bool value);
bool aeron_driver_context_get_term_buffer_sparse_file(aeron_driver_context_t *context);

/**
 * Should storage checks should be performed when allocating files.
 */
#define AERON_PERFORM_STORAGE_CHECKS_ENV_VAR "AERON_PERFORM_STORAGE_CHECKS"

int aeron_driver_context_set_perform_storage_checks(aeron_driver_context_t *context, bool value);
bool aeron_driver_context_get_perform_storage_checks(aeron_driver_context_t *context);

/**
 * Should a spy subscription simulate a connection to a network publication.
 */
#define AERON_SPIES_SIMULATE_CONNECTION_ENV_VAR "AERON_SPIES_SIMULATE_CONNECTION"

int aeron_driver_context_set_spies_simulate_connection(aeron_driver_context_t *context, bool value);
bool aeron_driver_context_get_spies_simulate_connection(aeron_driver_context_t *context);

/**
 * Page size for alignment of all files.
 */
#define AERON_FILE_PAGE_SIZE_ENV_VAR "AERON_FILE_PAGE_SIZE"

int aeron_driver_context_set_file_page_size(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_file_page_size(aeron_driver_context_t *context);

/**
 * Length (in bytes) of the maximum transmission unit of the publication.
 */
#define AERON_MTU_LENGTH_ENV_VAR "AERON_MTU_LENGTH"

int aeron_driver_context_set_mtu_length(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_mtu_length(aeron_driver_context_t *context);

/**
 * Length (in bytes) of the maximum transmission unit of the IPC publication.
 */
#define AERON_IPC_MTU_LENGTH_ENV_VAR "AERON_IPC_MTU_LENGTH"

int aeron_driver_context_set_ipc_mtu_length(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_ipc_mtu_length(aeron_driver_context_t *context);

/**
 * Window limit on IPC Publication side.
 */
#define AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR "AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH"

int aeron_driver_context_set_ipc_publication_term_window_length(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_ipc_publication_term_window_length(aeron_driver_context_t *context);

/**
 * Window limit on Publication side.
 */
#define AERON_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR "AERON_PUBLICATION_TERM_WINDOW_LENGTH"

int aeron_driver_context_set_publication_term_window_length(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_publication_term_window_length(aeron_driver_context_t *context);

/**
 * Linger timeout in nanoseconds on publications.
 */
#define AERON_PUBLICATION_LINGER_TIMEOUT_ENV_VAR "AERON_PUBLICATION_LINGER_TIMEOUT"

int aeron_driver_context_set_publication_linger_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_publication_linger_timeout_ns(aeron_driver_context_t *context);

/**
 * SO_RCVBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
 */
#define AERON_SOCKET_SO_RCVBUF_ENV_VAR "AERON_SOCKET_SO_RCVBUF"

int aeron_driver_context_set_socket_so_rcvbuf(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_socket_so_rcvbuf(aeron_driver_context_t *context);

/**
 * SO_SNDBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
 */
#define AERON_SOCKET_SO_SNDBUF_ENV_VAR "AERON_SOCKET_SO_SNDBUF"

int aeron_driver_context_set_socket_so_sndbuf(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_socket_so_sndbuf(aeron_driver_context_t *context);

/**
 * IP_MULTICAST_TTL setting on outgoing UDP sockets.
 */
#define AERON_SOCKET_MULTICAST_TTL_ENV_VAR "AERON_SOCKET_MULTICAST_TTL"

int aeron_driver_context_set_socket_multicast_ttl(aeron_driver_context_t *context, uint8_t value);
uint8_t aeron_driver_context_get_socket_multicast_ttl(aeron_driver_context_t *context);

/**
 * Ratio of sending data to polling status messages in the Sender.
 */
#define AERON_SEND_TO_STATUS_POLL_RATIO_ENV_VAR "AERON_SEND_TO_STATUS_POLL_RATIO"

int aeron_driver_context_set_send_to_status_poll_ratio(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_send_to_status_poll_ratio(aeron_driver_context_t *context);

/**
 * Status Message timeout in nanoseconds.
 */
#define AERON_RCV_STATUS_MESSAGE_TIMEOUT_ENV_VAR "AERON_RCV_STATUS_MESSAGE_TIMEOUT"

int aeron_driver_context_set_rcv_status_message_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_rcv_status_message_timeout_ns(aeron_driver_context_t *context);

typedef struct aeron_flow_control_strategy_stct aeron_flow_control_strategy_t;

typedef struct aeron_udp_channel_stct aeron_udp_channel_t;

typedef int (*aeron_flow_control_strategy_supplier_func_t)(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_length);

#define AERON_MULTICAST_MIN_FLOW_CONTROL_STRATEGY_NAME "multicast_min"
#define AERON_MULTICAST_MAX_FLOW_CONTROL_STRATEGY_NAME "multicast_max"
#define AERON_MULTICAST_TAGGED_FLOW_CONTROL_STRATEGY_NAME "multicast_tagged"
#define AERON_UNICAST_MAX_FLOW_CONTROL_STRATEGY_NAME "unicast_max"

/**
 * Return a flow control strategy supplier function pointer associated with the given name. This only will find
 * strategies built into the driver and will not try to dynamically load nor find any in the current executable.
 *
 * @param name of the strategy
 * @return function pointer to supplier associated with the name
 */
aeron_flow_control_strategy_supplier_func_t aeron_flow_control_strategy_supplier_by_name(const char *name);

/**
 * Supplier for flow control structure to be employed for multicast channels.
 */
#define AERON_MULTICAST_FLOWCONTROL_SUPPLIER_ENV_VAR "AERON_MULTICAST_FLOWCONTROL_SUPPLIER"

int aeron_driver_context_set_multicast_flowcontrol_supplier(
    aeron_driver_context_t *context, aeron_flow_control_strategy_supplier_func_t value);
aeron_flow_control_strategy_supplier_func_t aeron_driver_context_get_multicast_flowcontrol_supplier(
    aeron_driver_context_t *context);

/**
 * Supplier for flow control structure to be employed for unicast channels.
 */
#define AERON_UNICAST_FLOWCONTROL_SUPPLIER_ENV_VAR "AERON_UNICAST_FLOWCONTROL_SUPPLIER"

int aeron_driver_context_set_unicast_flowcontrol_supplier(
    aeron_driver_context_t *context, aeron_flow_control_strategy_supplier_func_t value);
aeron_flow_control_strategy_supplier_func_t aeron_driver_context_get_unicast_flowcontrol_supplier(
    aeron_driver_context_t *context);

/**
 * Image liveness timeout in nanoseconds
 */
#define AERON_IMAGE_LIVENESS_TIMEOUT_ENV_VAR "AERON_IMAGE_LIVENESS_TIMEOUT"

int aeron_driver_context_set_image_liveness_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_image_liveness_timeout_ns(aeron_driver_context_t *context);

/**
 * Length of the initial window which must be sufficient for Bandwidth Delay Product (BDP).
 */
#define AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR "AERON_RCV_INITIAL_WINDOW_LENGTH"

int aeron_driver_context_set_rcv_initial_window_length(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_rcv_initial_window_length(aeron_driver_context_t *context);

/**
 * Supplier for congestion control structure to be employed for Images.
 */
#define AERON_CONGESTIONCONTROL_SUPPLIER_ENV_VAR "AERON_CONGESTIONCONTROL_SUPPLIER"

typedef struct aeron_congestion_control_strategy_stct aeron_congestion_control_strategy_t;
typedef struct aeron_counters_manager_stct aeron_counters_manager_t;
struct sockaddr_storage;

typedef int (*aeron_congestion_control_strategy_supplier_func_t)(
    aeron_congestion_control_strategy_t **strategy,
    size_t channel_length,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager);

int aeron_driver_context_set_congestioncontrol_supplier(
    aeron_driver_context_t *context, aeron_congestion_control_strategy_supplier_func_t value);
aeron_congestion_control_strategy_supplier_func_t aeron_driver_context_get_congestioncontrol_supplier(
    aeron_driver_context_t *context);

/**
 * Length (in bytes) of the buffer for the loss report log.
 */
#define AERON_LOSS_REPORT_BUFFER_LENGTH_ENV_VAR "AERON_LOSS_REPORT_BUFFER_LENGTH"

int aeron_driver_context_set_loss_report_buffer_length(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_loss_report_buffer_length(aeron_driver_context_t *context);

/**
 * Timeout for publication unblock in nanoseconds.
 */
#define AERON_PUBLICATION_UNBLOCK_TIMEOUT_ENV_VAR "AERON_PUBLICATION_UNBLOCK_TIMEOUT"

int aeron_driver_context_set_publication_unblock_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_publication_unblock_timeout_ns(aeron_driver_context_t *context);

/**
 * Timeout for publication connection in nanoseconds.
 */
#define AERON_PUBLICATION_CONNECTION_TIMEOUT_ENV_VAR "AERON_PUBLICATION_CONNECTION_TIMEOUT"

int aeron_driver_context_set_publication_connection_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_publication_connection_timeout_ns(aeron_driver_context_t *context);

/**
 * Interval (in nanoseconds) between checks for timers and timeouts.
 */
#define AERON_TIMER_INTERVAL_ENV_VAR "AERON_TIMER_INTERVAL"

int aeron_driver_context_set_timer_interval_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_timer_interval_ns(aeron_driver_context_t *context);

/**
 * Idle strategy to be employed by Sender for DEDICATED Threading Mode.
 */
#define AERON_SENDER_IDLE_STRATEGY_ENV_VAR "AERON_SENDER_IDLE_STRATEGY"

int aeron_driver_context_set_sender_idle_strategy(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_sender_idle_strategy(aeron_driver_context_t *context);

/**
 * Idle strategy to be employed by Conductor for DEDICATED or SHARED_NETWORK Threading Mode.
 */
#define AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR "AERON_CONDUCTOR_IDLE_STRATEGY"

int aeron_driver_context_set_conductor_idle_strategy(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_conductor_idle_strategy(aeron_driver_context_t *context);

/**
 * Idle strategy to be employed by Receiver for DEDICATED Threading Mode.
 */
#define AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR "AERON_RECEIVER_IDLE_STRATEGY"

int aeron_driver_context_set_receiver_idle_strategy(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_receiver_idle_strategy(aeron_driver_context_t *context);

/**
 * Idle strategy to be employed by Sender and Receiver for SHARED_NETWORK Threading Mode.
 */
#define AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR "AERON_SHAREDNETWORK_IDLE_STRATEGY"

int aeron_driver_context_set_sharednetwork_idle_strategy(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_sharednetwork_idle_strategy(aeron_driver_context_t *context);

/**
 * Idle strategy to be employed by Conductor, Sender, and Receiver for SHARED Threading Mode.
 */
#define AERON_SHARED_IDLE_STRATEGY_ENV_VAR "AERON_SHARED_IDLE_STRATEGY"

int aeron_driver_context_set_shared_idle_strategy(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_shared_idle_strategy(aeron_driver_context_t *context);

/**
 * Idle strategy init args to be employed by Sender for DEDICATED Threading Mode.
 */
#define AERON_SENDER_IDLE_STRATEGY_INIT_ARGS_ENV_VAR "AERON_SENDER_IDLE_STRATEGY_INIT_ARGS"

int aeron_driver_context_set_sender_idle_strategy_init_args(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_sender_idle_strategy_init_args(aeron_driver_context_t *context);

/**
 * Idle strategy init args to be employed by Conductor for DEDICATED or SHARED_NETWORK Threading Mode.
 */
#define AERON_CONDUCTOR_IDLE_STRATEGY_INIT_ARGS_ENV_VAR "AERON_CONDUCTOR_IDLE_STRATEGY_INIT_ARGS"

int aeron_driver_context_set_conductor_idle_strategy_init_args(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_conductor_idle_strategy_init_args(aeron_driver_context_t *context);

/**
 * Idle strategy init args to be employed by Receiver for DEDICATED Threading Mode.
 */
#define AERON_RECEIVER_IDLE_STRATEGY_INIT_ARGS_ENV_VAR "AERON_RECEIVER_IDLE_STRATEGY_INIT_ARGS"

int aeron_driver_context_set_receiver_idle_strategy_init_args(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_receiver_idle_strategy_init_args(aeron_driver_context_t *context);

/**
 * Idle strategy init args to be employed by Sender and Receiver for SHARED_NETWORK Threading Mode.
 */
#define AERON_SHAREDNETWORK_IDLE_STRATEGY_INIT_ARGS_ENV_VAR "AERON_SHAREDNETWORK_IDLE_STRATEGY_INIT_ARGS"

int aeron_driver_context_set_sharednetwork_idle_strategy_init_args(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_sharednetwork_idle_strategy_init_args(aeron_driver_context_t *context);

/**
 * Idle strategy init args to be employed by Conductor, Sender, and Receiver for SHARED Threading Mode.
 */
#define AERON_SHARED_IDLE_STRATEGY_ENV_INIT_ARGS_VAR "AERON_SHARED_IDLE_STRATEGY_INIT_ARGS"

int aeron_driver_context_set_shared_idle_strategy_init_args(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_shared_idle_strategy_init_args(aeron_driver_context_t *context);

/**
 * Function name to call on start of each agent.
 */
#define AERON_AGENT_ON_START_FUNCTION_ENV_VAR "AERON_AGENT_ON_START_FUNCTION"

typedef void (*aeron_agent_on_start_func_t)(void *state, const char *role_name);

int aeron_driver_context_set_agent_on_start_function(
    aeron_driver_context_t *context, aeron_agent_on_start_func_t value, void *state);
aeron_agent_on_start_func_t aeron_driver_context_get_agent_on_start_function(aeron_driver_context_t *context);
void *aeron_driver_context_get_agent_on_start_state(aeron_driver_context_t *context);

/**
 * Timeout for freed counters before they can be reused.
 */
#define AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_ENV_VAR "AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT"

int aeron_driver_context_set_counters_free_to_reuse_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_counters_free_to_reuse_timeout_ns(aeron_driver_context_t *context);

/**
 * Timeout for a receiver to be tracked.
 */
#define AERON_MIN_MULTICAST_FLOW_CONTROL_RECEIVER_TIMEOUT_ENV_VAR "AERON_MIN_MULTICAST_FLOW_CONTROL_RECEIVER_TIMEOUT"

int aeron_driver_context_set_flow_control_receiver_timeout_ns(
    aeron_driver_context_t *context,
    uint64_t value);

uint64_t aeron_driver_context_get_flow_control_receiver_timeout_ns(aeron_driver_context_t *context);

/**
 * Default receiver tag for publishers to group endpoints by using tagged flow control.
 */
#define AERON_FLOW_CONTROL_GROUP_TAG_ENV_VAR "AERON_FLOW_CONTROL_GROUP_TAG"

int aeron_driver_context_set_flow_control_group_tag(aeron_driver_context_t *context, int64_t value);
int64_t aeron_driver_context_get_flow_control_group_tag(aeron_driver_context_t *context);

/**
 * Default required group size to use in tagged multicast flow control.
 */
#define AERON_FLOW_CONTROL_GROUP_MIN_SIZE_ENV_VAR "AERON_FLOW_CONTROL_GROUP_MIN_SIZE"

int aeron_driver_context_set_flow_control_group_min_size(aeron_driver_context_t *context, int32_t value);
int32_t aeron_driver_context_get_flow_control_group_min_size(aeron_driver_context_t *context);

/**
 * Default receiver tag to be sent on status messages from channel to handle tagged flow control.
 */
#define AERON_RECEIVER_GROUP_TAG_ENV_VAR "AERON_RECEIVER_GROUP_TAG"

int aeron_driver_context_set_receiver_group_tag(aeron_driver_context_t *context, bool is_present, int64_t value);
bool aeron_driver_context_get_receiver_group_tag_is_present(aeron_driver_context_t *context);
int64_t aeron_driver_context_get_receiver_group_tag_value(aeron_driver_context_t *context);

/**
 * Function name to call for termination validation.
 */
#define AERON_DRIVER_TERMINATION_VALIDATOR_ENV_VAR "AERON_DRIVER_TERMINATION_VALIDATOR"

typedef bool (*aeron_driver_termination_validator_func_t)(void *state, uint8_t *buffer, int32_t length);

int aeron_driver_context_set_driver_termination_validator(
    aeron_driver_context_t *context, aeron_driver_termination_validator_func_t value, void *state);
aeron_driver_termination_validator_func_t aeron_driver_context_get_driver_termination_validator(
    aeron_driver_context_t *context);
void *aeron_driver_context_get_driver_termination_validator_state(
    aeron_driver_context_t *context);

typedef void (*aeron_driver_termination_hook_func_t)(void *clientd);

int aeron_driver_context_set_driver_termination_hook(
    aeron_driver_context_t *context, aeron_driver_termination_hook_func_t value, void *state);
aeron_driver_termination_hook_func_t aeron_driver_context_get_driver_termination_hook(
    aeron_driver_context_t *context);
void *aeron_driver_context_get_driver_termination_hook_state(
    aeron_driver_context_t *context);

/**
 * Should the driver print its configuration on start to stdout.
 */
#define AERON_PRINT_CONFIGURATION_ON_START_ENV_VAR "AERON_PRINT_CONFIGURATION"

int aeron_driver_context_set_print_configuration(aeron_driver_context_t *context, bool value);
bool aeron_driver_context_get_print_configuration(aeron_driver_context_t *context);

/**
 * Property name for default boolean value for if a stream is reliable. True to NAK, false to gap fill.
 */
#define AERON_RELIABLE_STREAM_ENV_VAR "AERON_RELIABLE_STREAM"

int aeron_driver_context_set_reliable_stream(aeron_driver_context_t *context, bool value);
bool aeron_driver_context_get_reliable_stream(aeron_driver_context_t *context);

/**
 * Property name for default boolean value for if subscriptions should have a tether for flow control.
 */
#define AERON_TETHER_SUBSCRIPTIONS_ENV_VAR "AERON_TETHER_SUBSCRIPTIONS"

int aeron_driver_context_set_tether_subscriptions(aeron_driver_context_t *context, bool value);
bool aeron_driver_context_get_tether_subscriptions(aeron_driver_context_t *context);

/**
 * Untethered subscriptions window limit timeout after which they are removed from flow control.
 */
#define AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_ENV_VAR "AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT"


int aeron_driver_context_set_untethered_window_limit_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_untethered_window_limit_timeout_ns(aeron_driver_context_t *context);

/**
 * Untethered subscriptions resting timeout before they are allowed to re join an active stream.
 */
#define AERON_UNTETHERED_RESTING_TIMEOUT_ENV_VAR "AERON_UNTETHERED_RESTING_TIMEOUT"

int aeron_driver_context_set_untethered_resting_timeout_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_untethered_resting_timeout_ns(aeron_driver_context_t *context);

/**
 * Timeout in which the driver is expected to respond or heartbeat.
 */
#define AERON_DRIVER_TIMEOUT_ENV_VAR "AERON_DRIVER_TIMEOUT"

int aeron_driver_context_set_driver_timeout_ms(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_driver_timeout_ms(aeron_driver_context_t *context);

/**
 * Expected size of multicast receiver groups property name.
 */
#define AERON_NAK_MULTICAST_GROUP_SIZE_ENV_VAR "AERON_NAK_MULTICAST_GROUP_SIZE"

int aeron_driver_context_set_nak_multicast_group_size(aeron_driver_context_t *context, size_t value);
size_t aeron_driver_context_get_nak_multicast_group_size(aeron_driver_context_t *context);

/**
 * Max backoff time for multicast NAK delay randomisation in nanoseconds.
 */
#define AERON_NAK_MULTICAST_MAX_BACKOFF_ENV_VAR "AERON_NAK_MULTICAST_MAX_BACKOFF"

int aeron_driver_context_set_nak_multicast_max_backoff_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_nak_multicast_max_backoff_ns(aeron_driver_context_t *context);

/**
 * How long to delay before resending a NAK.
 */
#define AERON_NAK_UNICAST_DELAY_ENV_VAR "AERON_NAK_UNICAST_DELAY"

int aeron_driver_context_set_nak_unicast_delay_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_nak_unicast_delay_ns(aeron_driver_context_t *context);

/**
 * How long to delay before sending a retransmit following a NAK.
 */
#define AERON_RETRANSMIT_UNICAST_DELAY_ENV_VAR "AERON_RETRANSMIT_UNICAST_DELAY"

int aeron_driver_context_set_retransmit_unicast_delay_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_retransmit_unicast_delay_ns(aeron_driver_context_t *context);

/**
 * How long to linger after delay on a NAK.
 */
#define AERON_RETRANSMIT_UNICAST_LINGER_ENV_VAR "AERON_RETRANSMIT_UNICAST_LINGER"

int aeron_driver_context_set_retransmit_unicast_linger_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_retransmit_unicast_linger_ns(aeron_driver_context_t *context);

/**
 * Group semantics for network subscriptions.
 */
#define AERON_RECEIVER_GROUP_CONSIDERATION_ENV_VAR "AERON_RECEIVER_GROUP_CONSIDERATION"

typedef enum aeron_inferable_boolean_enum
{
    AERON_FORCE_FALSE,
    AERON_FORCE_TRUE,
    AERON_INFER
}
aeron_inferable_boolean_t;

int aeron_driver_context_set_receiver_group_consideration(
    aeron_driver_context_t *context, aeron_inferable_boolean_t value);
aeron_inferable_boolean_t aeron_driver_context_get_receiver_group_consideration(aeron_driver_context_t *context);

/**
 * Property name for default boolean value for if a stream is rejoinable. True to allow rejoin, false to not.
 * */
#define AERON_REJOIN_STREAM_ENV_VAR "AERON_REJOIN_STREAM"

int aeron_driver_context_set_rejoin_stream(aeron_driver_context_t *context, bool value);
bool aeron_driver_context_get_rejoin_stream(aeron_driver_context_t *context);

#define AERON_IPC_CHANNEL "aeron:ipc"
#define AERON_IPC_CHANNEL_LEN strlen(AERON_IPC_CHANNEL)
#define AERON_SPY_PREFIX "aeron-spy:"
#define AERON_SPY_PREFIX_LEN strlen(AERON_SPY_PREFIX)

/**
 * Bindings for UDP Channel Transports.
 */
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_ENV_VAR "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA"

typedef struct aeron_udp_channel_transport_bindings_stct aeron_udp_channel_transport_bindings_t;

int aeron_driver_context_set_udp_channel_transport_bindings(
    aeron_driver_context_t *context, aeron_udp_channel_transport_bindings_t *value);
aeron_udp_channel_transport_bindings_t *aeron_driver_context_get_udp_channel_transport_bindings(
    aeron_driver_context_t *context);

#define AERON_UDP_CHANNEL_OUTGOING_INTERCEPTORS_ENV_VAR "AERON_UDP_CHANNEL_OUTGOING_INTERCEPTORS"
#define AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR "AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS"

typedef struct aeron_udp_channel_interceptor_bindings_stct aeron_udp_channel_interceptor_bindings_t;

int aeron_driver_context_set_udp_channel_outgoing_interceptors(
    aeron_driver_context_t *context, aeron_udp_channel_interceptor_bindings_t *value);
aeron_udp_channel_interceptor_bindings_t *aeron_driver_context_get_udp_channel_outgoing_interceptors(
    aeron_driver_context_t *context);

int aeron_driver_context_set_udp_channel_incoming_interceptors(
    aeron_driver_context_t *context, aeron_udp_channel_interceptor_bindings_t *value);
aeron_udp_channel_interceptor_bindings_t *aeron_driver_context_get_udp_channel_incoming_interceptors(
    aeron_driver_context_t *context);

#define AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_ENV_VAR "AERON_PUBLICATION_RESERVED_SESSION_ID_LOW"

int aeron_driver_context_set_publication_reserved_session_id_low(aeron_driver_context_t *context, int32_t value);
int32_t aeron_driver_context_get_publication_reserved_session_id_low(aeron_driver_context_t *context);

#define AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_ENV_VAR "AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH"

int aeron_driver_context_set_publication_reserved_session_id_high(aeron_driver_context_t *context, int32_t value);
int32_t aeron_driver_context_get_publication_reserved_session_id_high(aeron_driver_context_t *context);

typedef struct aeron_name_resolver_stct aeron_name_resolver_t;
typedef int (*aeron_name_resolver_supplier_func_t)(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context);

/**
 * Set the name of the MediaDriver for name resolver purposes.
 */
#define AERON_DRIVER_RESOLVER_NAME_ENV_VAR "AERON_DRIVER_RESOLVER_NAME"

int aeron_driver_context_set_resolver_name(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_resolver_name(aeron_driver_context_t *context);

/**
* The interface of the MediaDriver for name resolver purposes.
*
* The format is hostname:port and follows the URI format for the interface parameter.
*/
#define AERON_DRIVER_RESOLVER_INTERFACE_ENV_VAR "AERON_DRIVER_RESOLVER_INTERFACE"

int aeron_driver_context_set_resolver_interface(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_resolver_interface(aeron_driver_context_t *context);

/**
 * Get the bootstrap neighbor of the {@link MediaDriver} for name resolver purposes.
 *
 * The format is hostname:port and follows the URI format for the endpoint parameter.
 */
#define AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_ENV_VAR "AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR"

int aeron_driver_context_set_resolver_bootstrap_neighbor(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_resolver_bootstrap_neighbor(aeron_driver_context_t *context);

/**
* Specify the name of the name resolver (supplier) to be used by this media driver
*/
#define AERON_NAME_RESOLVER_SUPPLIER_ENV_VAR "AERON_NAME_RESOLVER_SUPPLIER"
#define AERON_NAME_RESOLVER_SUPPLIER_DEFAULT "default"

int aeron_driver_context_set_name_resolver_supplier(
    aeron_driver_context_t *context,
    aeron_name_resolver_supplier_func_t value);
aeron_name_resolver_supplier_func_t aeron_driver_context_get_name_resolver_supplier(aeron_driver_context_t *context);

/**
 * Specify the name of the name resolver (supplier) to be used by this media driver
 */
#define AERON_NAME_RESOLVER_INIT_ARGS_ENV_VAR "AERON_NAME_RESOLVER_INIT_ARGS"

int aeron_driver_context_set_name_resolver_init_args(aeron_driver_context_t *context, const char *value);
const char *aeron_driver_context_get_name_resolver_init_args(aeron_driver_context_t *context);

/**
 * Specify the interval which checks for re-resolutions of names occurs.
 */
#define AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL_ENV_VAR "AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL"

int aeron_driver_context_set_re_resolution_check_interval_ns(aeron_driver_context_t *context, uint64_t value);
uint64_t aeron_driver_context_get_re_resolution_check_interval_ns(aeron_driver_context_t *context);

/**
 * Return full version and build string.
 *
 * @return full version and build string.
 */
const char *aeron_version_full();

/**
 * Return major version number.
 *
 * @return major version number.
 */
int aeron_version_major();

/**
 * Return minor version number.
 *
 * @return minor version number.
 */
int aeron_version_minor();

/**
 * Return patch version number.
 *
 * @return patch version number.
 */
int aeron_version_patch();

/**
 * Create a aeron_driver_context_t struct and initialize with default values.
 *
 * @param context to create and initialize
 * @return 0 for success and -1 for error.
 */
int aeron_driver_context_init(aeron_driver_context_t **context);

/**
 * Close and delete aeron_driver_context_t struct.
 *
 * @param context to close and delete
 * @return 0 for success and -1 for error.
 */
int aeron_driver_context_close(aeron_driver_context_t *context);

/**
 * Create a aeron_driver_t struct and initialize from the aeron_driver_context_t struct.
 *
 * The given aeron_driver_context_t struct will be used exclusively by the driver. Do not reuse between drivers.
 *
 * @param driver  to create and initialize.
 * @param context to use for initialization.
 * @return 0 for success and -1 for error.
 */
int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context);

/**
 * Start an aeron_driver_t given the threading mode. This may spawn threads for the Sender, Receiver, and Conductor
 * depending on threading mode used.
 *
 * @param driver to start.
 * @param manual_main_loop to be called by the caller for the Conductor do_work cycle.
 * @return 0 for success and -1 for error.
 */
int aeron_driver_start(aeron_driver_t *driver, bool manual_main_loop);

/**
 * Call the Conductor (or Shared) main do_work duty cycle once.
 *
 * Driver must have been created with manual_main_loop set to true.
 *
 * @param driver to call do_work duty cycle on.
 * @return 0 for success and -1 for error.
 */
int aeron_driver_main_do_work(aeron_driver_t *driver);

/**
 * Call the Conductor (or Shared) Idle Strategy.
 *
 * @param driver to idle.
 * @param work_count to pass to idle strategy.
 */
void aeron_driver_main_idle_strategy(aeron_driver_t *driver, int work_count);

/**
 * Close and delete aeron_driver_t struct.
 *
 * @param driver to close and delete
 * @return 0 for success and -1 for error.
 */
int aeron_driver_close(aeron_driver_t *driver);

/**
 * Delete the given aeron directory.
 *
 * @param dirname to delete.
 * @return 0 for success and -1 for error.
 */
int aeron_delete_directory(const char *dirname);

/**
 * Clock function used by aeron.
 */
typedef int64_t (*aeron_clock_func_t)();

/**
 * Function to return logging information.
 */
typedef void (*aeron_log_func_t)(const char *);

/**
 * Determine if an aeron driver is using a given aeron directory.
 *
 * @param dirname  for aeron directory
 * @param timeout_ms  to use to determine activity for aeron directory
 * @param log_func to call during activity check to log diagnostic information.
 * @return true for active driver or false for no active driver.
 */
bool aeron_is_driver_active(const char *dirname, int64_t timeout_ms, aeron_log_func_t log_func);

/**
 * Load properties from a string containing name=value pairs and set appropriate environment variables for the
 * process so that subsequent calls to aeron_driver_context_init will use those values.
 *
 * @param buffer containing properties and values.
 * @return 0 for success and -1 for error.
 */
int aeron_properties_buffer_load(const char *buffer);

/**
 * Load properties file and set appropriate environment variables for the process so that subsequent
 * calls to aeron_driver_context_init will use those values.
 *
 * @param filename to load.
 * @return 0 for success and -1 for error.
 */
int aeron_properties_file_load(const char *filename);

/**
 * Load properties from HTTP URL and set environment variables for the process so that subsequent
 * calls to aeron_driver_context_init will use those values.
 *
 * @param url to attempt to retrieve and load.
 * @return 0 for success and -1 for error.
 */
int aeron_properties_http_load(const char *url);

/**
 * Load properties based on URL or filename. If string contains file or http URL, it will attempt
 * to load properties from a file or http as indicated. If not a URL, then it will try to load the string
 * as a filename.
 *
 * @param url_or_filename to load properties from.
 * @return 0 for success and -1 for error.
 */
int aeron_properties_load(const char *url_or_filename);

/**
 * Return current aeron error code (errno) for calling thread.
 *
 * @return aeron error code for calling thread.
 */
int aeron_errcode();

/**
 * Return the current aeron error message for calling thread.
 *
 * @return aeron error message for calling thread.
 */
const char *aeron_errmsg();

#ifdef __cplusplus
}
#endif

#endif //AERON_AERONMD_H
