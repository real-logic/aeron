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

#ifndef AERON_DRIVER_CONTEXT_H
#define AERON_DRIVER_CONTEXT_H

#include <concurrent/aeron_distinct_error_log.h>
#include <media/aeron_udp_channel_transport_bindings.h>
#include "aeron_driver_common.h"
#include "aeronmd.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_fileutil.h"
#include "concurrent/aeron_spsc_concurrent_array_queue.h"
#include "concurrent/aeron_mpsc_concurrent_array_queue.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "aeron_flow_control.h"
#include "aeron_congestion_control.h"
#include "aeron_agent.h"

#define AERON_CNC_FILE "cnc.dat"
#define AERON_LOSS_REPORT_FILE "loss-report.dat"

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_cnc_metadata_stct
{
    int32_t cnc_version;
    int32_t to_driver_buffer_length;
    int32_t to_clients_buffer_length;
    int32_t counter_metadata_buffer_length;
    int32_t counter_values_buffer_length;
    int32_t error_log_buffer_length;
    int64_t client_liveness_timeout;
    int64_t start_timestamp;
    int64_t pid;
}
aeron_cnc_metadata_t;
#pragma pack(pop)

#define AERON_CNC_VERSION_AND_META_DATA_LENGTH (AERON_ALIGN(sizeof(aeron_cnc_metadata_t), AERON_CACHE_LINE_LENGTH * 2))

#define AERON_COMMAND_QUEUE_CAPACITY (256)

typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

typedef struct aeron_driver_conductor_proxy_stct aeron_driver_conductor_proxy_t;
typedef struct aeron_driver_sender_proxy_stct aeron_driver_sender_proxy_t;
typedef struct aeron_driver_receiver_proxy_stct aeron_driver_receiver_proxy_t;

typedef aeron_rb_handler_t aeron_driver_conductor_to_driver_interceptor_func_t;
typedef void (*aeron_driver_conductor_to_client_interceptor_func_t)(
    aeron_driver_conductor_t *conductor, int32_t msg_type_id, const void *message, size_t length);

#define AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(m) (AERON_THREADING_MODE_SHARED == m || AERON_THREADING_MODE_INVOKER == m)

typedef struct aeron_driver_context_stct
{
    char *aeron_dir;                                        /* aeron.dir */
    aeron_threading_mode_t threading_mode;                  /* aeron.threading.mode = DEDICATED */
    aeron_inferable_boolean_t receiver_group_consideration; /* aeron.receiver.group.consideration = INFER */
    bool dirs_delete_on_start;                              /* aeron.dir.delete.on.start = false */
    bool dirs_delete_on_shutdown;                           /* aeron.dir.delete.on.shutdown = false */
    bool warn_if_dirs_exist;                                /* aeron.dir.warn.if.exists = false */
    bool term_buffer_sparse_file;                           /* aeron.term.buffer.sparse.file = false */
    bool perform_storage_checks;                            /* aeron.perform.storage.checks = true */
    bool spies_simulate_connection;                         /* aeron.spies.simulate.connection = false */
    bool print_configuration_on_start;                      /* aeron.print.configuration = false */
    bool reliable_stream;                                   /* aeron.reliable.stream = true */
    bool tether_subscriptions;                              /* aeron.tether.subscriptions = true */
    bool rejoin_stream;                                     /* aeron.rejoin.stream = true */
    uint64_t driver_timeout_ms;                             /* aeron.driver.timeout = 10s */
    uint64_t client_liveness_timeout_ns;                    /* aeron.client.liveness.timeout = 5s */
    uint64_t publication_linger_timeout_ns;                 /* aeron.publication.linger.timeout = 5s */
    uint64_t status_message_timeout_ns;                     /* aeron.rcv.status.message.timeout = 200ms */
    uint64_t image_liveness_timeout_ns;                     /* aeron.image.liveness.timeout = 10s */
    uint64_t publication_unblock_timeout_ns;                /* aeron.publication.unblock.timeout = 10s */
    uint64_t publication_connection_timeout_ns;             /* aeron.publication.connection.timeout = 5s */
    uint64_t timer_interval_ns;                             /* aeron.timer.interval = 1s */
    uint64_t counter_free_to_reuse_ns;                      /* aeron.counters.free.to.reuse.timeout = 1s */
    uint64_t untethered_window_limit_timeout_ns;            /* aeron.untethered.window.limit.timeout = 5s */
    uint64_t untethered_resting_timeout_ns;                 /* aeron.untethered.resting.timeout = 10s */
    uint64_t retransmit_unicast_delay_ns;                   /* aeron.retransmit.unicast.delay = 0 */
    uint64_t retransmit_unicast_linger_ns;                  /* aeron.retransmit.unicast.linger = 60ms */
    uint64_t nak_unicast_delay_ns;                          /* aeron.nak.unicast.delay = 60ms */
    uint64_t nak_multicast_max_backoff_ns;                  /* aeron.nak.multicast.max.backoff = 60ms */
    size_t to_driver_buffer_length;                         /* aeron.conductor.buffer.length = 1MB + trailer*/
    size_t to_clients_buffer_length;                        /* aeron.clients.buffer.length = 1MB + trailer */
    size_t counters_values_buffer_length;                   /* aeron.counters.buffer.length = 1MB */
    size_t error_buffer_length;                             /* aeron.error.buffer.length = 1MB */
    size_t term_buffer_length;                              /* aeron.term.buffer.length = 16MB */
    size_t ipc_term_buffer_length;                          /* aeron.ipc.term.buffer.length = 64MB */
    size_t mtu_length;                                      /* aeron.mtu.length = 1408 */
    size_t ipc_mtu_length;                                  /* aeron.ipc.mtu.length = 1408 */
    size_t ipc_publication_window_length;                   /* aeron.ipc.publication.term.window.length = 0 */
    size_t publication_window_length;                       /* aeron.publication.term.window.length = 0 */
    size_t socket_rcvbuf;                                   /* aeron.socket.so_rcvbuf = 128 * 1024 */
    size_t socket_sndbuf;                                   /* aeron.socket.so_sndbuf = 0 */
    size_t send_to_sm_poll_ratio;                           /* aeron.send.to.status.poll.ratio = 4 */
    size_t initial_window_length;                           /* aeron.rcv.initial.window.length = 128KB */
    size_t loss_report_length;                              /* aeron.loss.report.buffer.length = 1MB */
    size_t file_page_size;                                  /* aeron.file.page.size = 4KB */
    size_t nak_multicast_group_size;                        /* aeron.nak.multicast.group.size = 10 */
    int32_t publication_reserved_session_id_low;            /* aeron.publication.reserved.session.id.low = -1 */
    int32_t publication_reserved_session_id_high;           /* aeron.publication.reserved.session.id.high = 10000 */
    uint8_t multicast_ttl;                                  /* aeron.socket.multicast.ttl = 0 */

    aeron_mapped_file_t cnc_map;
    aeron_mapped_file_t loss_report;

    uint8_t *to_driver_buffer;
    uint8_t *to_clients_buffer;
    uint8_t *counters_values_buffer;
    uint8_t *counters_metadata_buffer;
    uint8_t *error_buffer;

    aeron_clock_func_t nano_clock;
    aeron_clock_func_t epoch_clock;
    aeron_clock_cache_t* cached_clock;

    aeron_spsc_concurrent_array_queue_t sender_command_queue;
    aeron_spsc_concurrent_array_queue_t receiver_command_queue;
    aeron_mpsc_concurrent_array_queue_t conductor_command_queue;

    aeron_agent_on_start_func_t agent_on_start_func;
    void *agent_on_start_state;

    aeron_idle_strategy_func_t conductor_idle_strategy_func;
    void *conductor_idle_strategy_state;
    char *conductor_idle_strategy_init_args;
    const char *conductor_idle_strategy_name;

    aeron_idle_strategy_func_t shared_idle_strategy_func;
    void *shared_idle_strategy_state;
    char *shared_idle_strategy_init_args;
    const char *shared_idle_strategy_name;

    aeron_idle_strategy_func_t shared_network_idle_strategy_func;
    void *shared_network_idle_strategy_state;
    char *shared_network_idle_strategy_init_args;
    const char *shared_network_idle_strategy_name;

    aeron_idle_strategy_func_t sender_idle_strategy_func;
    void *sender_idle_strategy_state;
    char *sender_idle_strategy_init_args;
    const char *sender_idle_strategy_name;

    aeron_idle_strategy_func_t receiver_idle_strategy_func;
    void *receiver_idle_strategy_state;
    char *receiver_idle_strategy_init_args;
    const char *receiver_idle_strategy_name;

    aeron_usable_fs_space_func_t usable_fs_space_func;
    aeron_map_raw_log_func_t map_raw_log_func;
    aeron_map_raw_log_close_func_t map_raw_log_close_func;

    aeron_flow_control_strategy_supplier_func_t unicast_flow_control_supplier_func;
    aeron_flow_control_strategy_supplier_func_t multicast_flow_control_supplier_func;

    aeron_congestion_control_strategy_supplier_func_t congestion_control_supplier_func;

    aeron_driver_conductor_proxy_t *conductor_proxy;
    aeron_driver_sender_proxy_t *sender_proxy;
    aeron_driver_receiver_proxy_t *receiver_proxy;

    aeron_counters_manager_t *counters_manager;
    aeron_distinct_error_log_t *error_log;

    aeron_driver_conductor_to_driver_interceptor_func_t to_driver_interceptor_func;
    aeron_driver_conductor_to_client_interceptor_func_t to_client_interceptor_func;

    aeron_driver_termination_validator_func_t termination_validator_func;
    void *termination_validator_state;

    aeron_driver_termination_hook_func_t termination_hook_func;
    void *termination_hook_state;

    aeron_udp_channel_transport_bindings_t *udp_channel_transport_bindings;
    aeron_udp_channel_interceptor_bindings_t *udp_channel_outgoing_interceptor_bindings;
    aeron_udp_channel_interceptor_bindings_t *udp_channel_incoming_interceptor_bindings;

    int64_t next_receiver_id;

    aeron_feedback_delay_generator_state_t unicast_delay_feedback_generator;
    aeron_feedback_delay_generator_state_t multicast_delay_feedback_generator;
}
aeron_driver_context_t;

aeron_inferable_boolean_t aeron_config_parse_inferable_boolean(
    const char *inferable_boolean, aeron_inferable_boolean_t def);

void aeron_driver_context_print_configuration(aeron_driver_context_t *context);

void aeron_driver_fill_cnc_metadata(aeron_driver_context_t *context);

int aeron_driver_context_validate_mtu_length(uint64_t mtu_length);

size_t aeron_cnc_length(aeron_driver_context_t *context);

inline int32_t aeron_cnc_version_volatile(aeron_cnc_metadata_t *metadata)
{
    int32_t cnc_version;
    AERON_GET_VOLATILE(cnc_version, metadata->cnc_version);
    return cnc_version;
}

inline void aeron_cnc_version_signal_cnc_ready(aeron_cnc_metadata_t *metadata, int32_t cnc_version)
{
    AERON_PUT_VOLATILE(metadata->cnc_version, cnc_version);
}

inline uint8_t *aeron_cnc_to_driver_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH;
}

inline uint8_t *aeron_cnc_to_clients_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH + metadata->to_driver_buffer_length;
}

inline uint8_t *aeron_cnc_counters_metadata_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length;
}

inline uint8_t *aeron_cnc_counters_values_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length +
        metadata->counter_metadata_buffer_length;
}

inline uint8_t *aeron_cnc_error_log_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length +
        metadata->counter_metadata_buffer_length +
        metadata->counter_values_buffer_length;
}

inline size_t aeron_cnc_computed_length(size_t total_length_of_buffers, size_t alignment)
{
    return AERON_ALIGN(AERON_CNC_VERSION_AND_META_DATA_LENGTH + total_length_of_buffers, alignment);
}

inline size_t aeron_producer_window_length(size_t producer_window_length, size_t term_length)
{
    size_t window_length = term_length / 2;

    if (0 != producer_window_length && producer_window_length < window_length)
    {
        window_length = producer_window_length;
    }

    return window_length;
}

#endif //AERON_DRIVER_CONTEXT_H
