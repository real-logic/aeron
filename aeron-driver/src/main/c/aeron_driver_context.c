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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

#include "util/aeron_platform.h"
#if  defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
#include <io.h>
#else
#include <unistd.h>
#endif

#include <inttypes.h>
#include <errno.h>
#include <math.h>
#include <limits.h>

#ifdef HAVE_UUID_H

#include <uuid/uuid.h>

#endif

#include "aeron_windows.h"
#include "util/aeron_error.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_parse_util.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver.h"
#include "aeron_driver_context.h"
#include "aeron_alloc.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_transmitter.h"
#include "aeron_agent.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_termination_validator.h"
#include "agent/aeron_driver_agent.h"

#if defined(__clang__)
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wunused-function"
#endif

inline static const char *tmp_dir()
{
#if defined(_MSC_VER)
    static char buff[MAX_PATH + 1];

    if (GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        return buff;
    }

    return NULL;
#else
    const char *dir = "/tmp";

    if (getenv("TMPDIR"))
    {
        dir = getenv("TMPDIR");
    }

    return dir;
#endif
}

inline static bool has_file_separator_at_end(const char *path)
{
#if defined(_MSC_VER)
    return path[strlen(path) - 1] == '\\';
#else
    return path[strlen(path) - 1] == '/';
#endif
}

#if defined(__clang__)
    #pragma clang diagnostic pop
#endif

inline static const char *username()
{
    const char *username = getenv("USER");
#if (_MSC_VER)
    if (NULL == username)
    {
        username = getenv("USERNAME");
        if (NULL == username)
        {
             username = "default";
        }
    }
#else
    if (NULL == username)
    {
        username = "default";
    }
#endif
    return username;
}

void aeron_config_prop_warning(const char *name, const char *str)
{
    char buffer[AERON_MAX_PATH];
    snprintf(buffer, sizeof(buffer) - 1, "WARNING: %s=%s is invalid, using default\n", name, str);
    fprintf(stderr, "%s", buffer);
}

bool aeron_config_parse_bool(const char *str, bool def)
{
    if (NULL != str)
    {
        if (strncmp(str, "1", 1) == 0 || strncmp(str, "on", 2) == 0 || strncmp(str, "true", 4) == 0)
        {
            return true;
        }

        if (strncmp(str, "0", 1) == 0 || strncmp(str, "off", 3) == 0 || strncmp(str, "false", 5) == 0)
        {
            return false;
        }
    }

    return def;
}

uint64_t aeron_config_parse_uint64(const char *name, const char *str, uint64_t def, uint64_t min, uint64_t max)
{
    uint64_t result = def;

    if (NULL != str)
    {
        errno = 0;
        char *end_ptr = NULL;
        uint64_t value = strtoull(str, NULL, 0);

        if ((0 == value && 0 != errno) || end_ptr == str)
        {
            aeron_config_prop_warning(name, str);
            value = def;
        }

        result = value;
        result = result > max ? max : result;
        result = result < min ? min : result;
    }

    return result;
}

int32_t aeron_config_parse_int32(const char *name, const char *str, int32_t def, int32_t min, int32_t max)
{
    int32_t result = def;

    if (NULL != str)
    {
        errno = 0;
        char *end_ptr = NULL;
        int64_t value = strtoll(str, NULL, 0);

        if ((0 == value && 0 != errno) || end_ptr == str || value < INT32_MIN || INT32_MAX < value)
        {
            aeron_config_prop_warning(name, str);
            value = def;
        }

        result = value > max ? max : (int32_t)value;
        result = value < min ? min : (int32_t)result;
    }

    return result;
}

uint64_t aeron_config_parse_size64(const char *name, const char *str, uint64_t def, uint64_t min, uint64_t max)
{
    uint64_t result = def;

    if (NULL != str)
    {
        uint64_t value = 0;

        if (-1 == aeron_parse_size64(str, &value))
        {
            aeron_config_prop_warning(name, str);
        }
        else
        {
            result = value;
            result = result > max ? max : result;
            result = result < min ? min : result;
        }
    }

    return result;
}

uint64_t aeron_config_parse_duration_ns(const char *name, const char *str, uint64_t def, uint64_t min, uint64_t max)
{
    uint64_t result = def;

    if (NULL != str)
    {
        uint64_t value = 0;

        if (-1 == aeron_parse_duration_ns(str, &value))
        {
            aeron_config_prop_warning(name, str);
        }
        else
        {
            result = value;
            result = result > max ? max : result;
            result = result < min ? min : result;
        }
    }

    return result;
}

aeron_threading_mode_t aeron_config_parse_threading_mode(const char *threading_mode, aeron_threading_mode_t def)
{
    aeron_threading_mode_t result = def;

    if (NULL != threading_mode)
    {
        if (strncmp(threading_mode, "SHARED", sizeof("SHARED")) == 0)
        {
            result = AERON_THREADING_MODE_SHARED;
        }
        else if (strncmp(threading_mode, "SHARED_NETWORK", sizeof("SHARED_NETWORK")) == 0)
        {
            result = AERON_THREADING_MODE_SHARED_NETWORK;
        }
        else if (strncmp(threading_mode, "DEDICATED", sizeof("DEDICATED")) == 0)
        {
            result = AERON_THREADING_MODE_DEDICATED;
        }
        else if (strncmp(threading_mode, "INVOKER", sizeof("INVOKER")) == 0)
        {
            result = AERON_THREADING_MODE_INVOKER;
        }
        else
        {
            aeron_config_prop_warning(AERON_THREADING_MODE_ENV_VAR, threading_mode);
        }
    }

    return result;
}

aeron_inferable_boolean_t aeron_config_parse_inferable_boolean(
    const char *inferable_boolean, aeron_inferable_boolean_t def)
{
    aeron_inferable_boolean_t result = def;

    if (NULL != inferable_boolean)
    {
        if (strncmp(inferable_boolean, "TRUE", sizeof("TRUE")) == 0)
        {
            result = AERON_FORCE_TRUE;
        }
        else if (strncmp(inferable_boolean, "INFER", sizeof("INFER")) == 0)
        {
            result = AERON_INFER;
        }
        else
        {
            result = AERON_FORCE_FALSE;
        }
    }

    return result;
}

#define AERON_CONFIG_GETENV_OR_DEFAULT(e, d) ((NULL == getenv(e)) ? (d) : getenv(e))
#define AERON_CONFIG_STRNDUP_GETENV_OR_NULL(e) ((NULL == getenv(e)) ? (NULL) : aeron_strndup(getenv(e), AERON_MAX_PATH))

static void aeron_driver_conductor_to_driver_interceptor_null(
    int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
}

static void aeron_driver_conductor_to_client_interceptor_null(
    aeron_driver_conductor_t *conductor, int32_t msg_type_id, const void *message, size_t length)
{
}

#define AERON_DIR_WARN_IF_EXISTS_DEFAULT false
#define AERON_THREADING_MODE_DEFAULT AERON_THREADING_MODE_DEDICATED
#define AERON_DIR_DELETE_ON_START_DEFAULT false
#define AERON_DIR_DELETE_ON_SHUTDOWN_DEFAULT false
#define AERON_TO_CONDUCTOR_BUFFER_LENGTH_DEFAULT (1024 * 1024 + AERON_RB_TRAILER_LENGTH)
#define AERON_TO_CLIENTS_BUFFER_LENGTH_DEFAULT (1024 * 1024 + AERON_BROADCAST_BUFFER_TRAILER_LENGTH)
#define AERON_COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT (1024 * 1024)
#define AERON_ERROR_BUFFER_LENGTH_DEFAULT (1024 * 1024)
#define AERON_CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT (5 * 1000 * 1000 * 1000LL)
#define AERON_TERM_BUFFER_LENGTH_DEFAULT (16 * 1024 * 1024)
#define AERON_IPC_TERM_BUFFER_LENGTH_DEFAULT (64 * 1024 * 1024)
#define AERON_TERM_BUFFER_SPARSE_FILE_DEFAULT (false)
#define AERON_PERFORM_STORAGE_CHECKS_DEFAULT (true)
#define AERON_SPIES_SIMULATE_CONNECTION_DEFAULT (false)
#define AERON_FILE_PAGE_SIZE_DEFAULT (4 * 1024)
#define AERON_MTU_LENGTH_DEFAULT (1408)
#define AERON_IPC_MTU_LENGTH_DEFAULT (1408)
#define AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT (0)
#define AERON_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT (0)
#define AERON_PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT (5 * 1000 * 1000 * 1000LL)
#define AERON_SOCKET_SO_RCVBUF_DEFAULT (128 * 1024)
#define AERON_SOCKET_SO_SNDBUF_DEFAULT (0)
#define AERON_SOCKET_MULTICAST_TTL_DEFAULT (0)
#define AERON_SEND_TO_STATUS_POLL_RATIO_DEFAULT (4)
#define AERON_RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT (200 * 1000 * 1000LL)
#define AERON_MULTICAST_FLOWCONTROL_SUPPLIER_DEFAULT ("aeron_max_multicast_flow_control_strategy_supplier")
#define AERON_UNICAST_FLOWCONTROL_SUPPLIER_DEFAULT ("aeron_unicast_flow_control_strategy_supplier")
#define AERON_CONGESTIONCONTROL_SUPPLIER_DEFAULT ("aeron_static_window_congestion_control_strategy_supplier")
#define AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT (10 * 1000 * 1000 * 1000LL)
#define AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT (128 * 1024)
#define AERON_LOSS_REPORT_BUFFER_LENGTH_DEFAULT (1024 * 1024)
#define AERON_PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT (10 * 1000 * 1000 * 1000LL)
#define AERON_PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT (5 * 1000 * 1000 * 1000LL)
#define AERON_TIMER_INTERVAL_NS_DEFAULT (1 * 1000 * 1000 * 1000LL)
#define AERON_IDLE_STRATEGY_BACKOFF_DEFAULT "aeron_idle_strategy_backoff"
#define AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_NS_DEFAULT (1 * 1000 * 1000 * 1000LL)
#define AERON_PRINT_CONFIGURATION_DEFAULT (false)
#define AERON_RELIABLE_STREAM_DEFAULT (true)
#define AERON_TETHER_SUBSCRIPTIONS_DEFAULT (true)
#define AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT (5 * 1000 * 1000 * 1000LL)
#define AERON_UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT (10 * 1000 * 1000 * 1000LL)
#define AERON_DRIVER_TIMEOUT_MS_DEFAULT (10 * 1000)
#define AERON_RETRANSMIT_UNICAST_DELAY_NS_DEFAULT (0)
#define AERON_RETRANSMIT_UNICAST_LINGER_NS_DEFAULT (60 * 1000 * 1000LL)
#define AERON_NAK_MULTICAST_GROUP_SIZE_DEFAULT (10)
#define AERON_NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT (60 * 1000 * 1000LL)
#define AERON_NAK_UNICAST_DELAY_NS_DEFAULT (60 * 1000 * 1000LL)
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT ("default")
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_INTERCEPTORS_DEFAULT ("")
#define AERON_RECEIVER_GROUP_CONSIDERATION_DEFAULT (AERON_INFER)
#define AERON_REJOIN_STREAM_DEFAULT (true)
#define AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT (-1)
#define AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT (10000)

int aeron_driver_context_init(aeron_driver_context_t **context)
{
    aeron_driver_context_t *_context = NULL;

    if (NULL == context)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_driver_context_init(NULL): %s", strerror(EINVAL));
        return -1;
    }

    if (aeron_alloc((void **)&_context, sizeof(aeron_driver_context_t)) < 0)
    {
        return -1;
    }

    _context->cnc_map.addr = NULL;
    _context->loss_report.addr = NULL;
    _context->aeron_dir = NULL;
    _context->conductor_proxy = NULL;
    _context->sender_proxy = NULL;
    _context->receiver_proxy = NULL;
    _context->counters_manager = NULL;
    _context->error_log = NULL;
    _context->udp_channel_outgoing_interceptor_bindings = NULL;
    _context->udp_channel_incoming_interceptor_bindings = NULL;

    if (aeron_alloc((void **)&_context->aeron_dir, AERON_MAX_PATH) < 0)
    {
        return -1;
    }

    if (aeron_spsc_concurrent_array_queue_init(&_context->sender_command_queue, AERON_COMMAND_QUEUE_CAPACITY) < 0)
    {
        return -1;
    }

    if (aeron_spsc_concurrent_array_queue_init(&_context->receiver_command_queue, AERON_COMMAND_QUEUE_CAPACITY) < 0)
    {
        return -1;
    }

    if (aeron_mpsc_concurrent_array_queue_init(&_context->conductor_command_queue, AERON_COMMAND_QUEUE_CAPACITY) < 0)
    {
        return -1;
    }

    _context->agent_on_start_func = NULL;
    _context->agent_on_start_state = NULL;

    if ((_context->unicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(
        AERON_UNICAST_FLOWCONTROL_SUPPLIER_DEFAULT)) == NULL)
    {
        return -1;
    }

    if ((_context->multicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(
        AERON_MULTICAST_FLOWCONTROL_SUPPLIER_DEFAULT)) == NULL)
    {
        return -1;
    }

    if ((_context->congestion_control_supplier_func = aeron_congestion_control_strategy_supplier_load(
        AERON_CONGESTIONCONTROL_SUPPLIER_DEFAULT)) == NULL)
    {
        return -1;
    }

#if defined(__linux__)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "/dev/shm/aeron-%s", username());
#elif defined(_MSC_VER)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "\\", username());
#else
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "/", username());
#endif

    _context->threading_mode = aeron_config_parse_threading_mode(
        getenv(AERON_THREADING_MODE_ENV_VAR), AERON_THREADING_MODE_DEFAULT);
    _context->receiver_group_consideration = aeron_config_parse_inferable_boolean(
        getenv(AERON_RECEIVER_GROUP_CONSIDERATION_ENV_VAR), AERON_RECEIVER_GROUP_CONSIDERATION_DEFAULT);
    _context->dirs_delete_on_start = AERON_DIR_DELETE_ON_START_DEFAULT;
    _context->dirs_delete_on_shutdown = AERON_DIR_DELETE_ON_SHUTDOWN_DEFAULT;
    _context->warn_if_dirs_exist = AERON_DIR_WARN_IF_EXISTS_DEFAULT;
    _context->term_buffer_sparse_file = AERON_TERM_BUFFER_SPARSE_FILE_DEFAULT;
    _context->perform_storage_checks = AERON_PERFORM_STORAGE_CHECKS_DEFAULT;
    _context->spies_simulate_connection = AERON_SPIES_SIMULATE_CONNECTION_DEFAULT;
    _context->print_configuration_on_start = AERON_PRINT_CONFIGURATION_DEFAULT;
    _context->reliable_stream = AERON_RELIABLE_STREAM_DEFAULT;
    _context->tether_subscriptions = AERON_TETHER_SUBSCRIPTIONS_DEFAULT;
    _context->rejoin_stream = AERON_REJOIN_STREAM_DEFAULT;
    _context->driver_timeout_ms = AERON_DRIVER_TIMEOUT_MS_DEFAULT;
    _context->to_driver_buffer_length = AERON_TO_CONDUCTOR_BUFFER_LENGTH_DEFAULT;
    _context->to_clients_buffer_length = AERON_TO_CLIENTS_BUFFER_LENGTH_DEFAULT;
    _context->counters_values_buffer_length = AERON_COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT;
    _context->error_buffer_length = AERON_ERROR_BUFFER_LENGTH_DEFAULT;
    _context->client_liveness_timeout_ns = AERON_CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT;
    _context->timer_interval_ns = AERON_TIMER_INTERVAL_NS_DEFAULT;
    _context->term_buffer_length = AERON_TERM_BUFFER_LENGTH_DEFAULT;
    _context->ipc_term_buffer_length = AERON_IPC_TERM_BUFFER_LENGTH_DEFAULT;
    _context->mtu_length = AERON_MTU_LENGTH_DEFAULT;
    _context->ipc_mtu_length = AERON_IPC_MTU_LENGTH_DEFAULT;
    _context->ipc_publication_window_length = AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT;
    _context->publication_window_length = AERON_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT;
    _context->publication_linger_timeout_ns = AERON_PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT;
    _context->socket_rcvbuf = AERON_SOCKET_SO_RCVBUF_DEFAULT;
    _context->socket_sndbuf = AERON_SOCKET_SO_SNDBUF_DEFAULT;
    _context->multicast_ttl = AERON_SOCKET_MULTICAST_TTL_DEFAULT;
    _context->send_to_sm_poll_ratio = AERON_SEND_TO_STATUS_POLL_RATIO_DEFAULT;
    _context->status_message_timeout_ns = AERON_RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT;
    _context->image_liveness_timeout_ns = AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT;
    _context->initial_window_length = AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT;
    _context->loss_report_length = AERON_LOSS_REPORT_BUFFER_LENGTH_DEFAULT;
    _context->file_page_size = AERON_FILE_PAGE_SIZE_DEFAULT;
    _context->publication_unblock_timeout_ns = AERON_PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT;
    _context->publication_connection_timeout_ns = AERON_PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT;
    _context->counter_free_to_reuse_ns = AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_NS_DEFAULT;
    _context->untethered_window_limit_timeout_ns = AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT;
    _context->untethered_resting_timeout_ns = AERON_UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT;
    _context->retransmit_unicast_delay_ns = AERON_RETRANSMIT_UNICAST_DELAY_NS_DEFAULT;
    _context->retransmit_unicast_linger_ns = AERON_RETRANSMIT_UNICAST_LINGER_NS_DEFAULT;
    _context->nak_multicast_group_size = AERON_NAK_MULTICAST_GROUP_SIZE_DEFAULT;
    _context->nak_multicast_max_backoff_ns = AERON_NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT;
    _context->nak_unicast_delay_ns = AERON_NAK_UNICAST_DELAY_NS_DEFAULT;
    _context->publication_reserved_session_id_low = AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT;
    _context->publication_reserved_session_id_high = AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT;

    char *value = NULL;

    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s", value);
    }

    if ((value = getenv(AERON_AGENT_ON_START_FUNCTION_ENV_VAR)))
    {
        if ((_context->agent_on_start_func = aeron_agent_on_start_load(value)) == NULL)
        {
            return -1;
        }
    }

    if ((value = getenv(AERON_UNICAST_FLOWCONTROL_SUPPLIER_ENV_VAR)))
    {
        if ((_context->unicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(value)) == NULL)
        {
            return -1;
        }
    }

    if ((value = getenv(AERON_MULTICAST_FLOWCONTROL_SUPPLIER_ENV_VAR)))
    {
        if ((_context->multicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(value)) == NULL)
        {
            return -1;
        }
    }

    if ((value = getenv(AERON_CONGESTIONCONTROL_SUPPLIER_ENV_VAR)))
    {
        if ((_context->congestion_control_supplier_func = aeron_congestion_control_strategy_supplier_load(value)) == NULL)
        {
            return -1;
        }
    }

    _context->dirs_delete_on_start = aeron_config_parse_bool(
        getenv(AERON_DIR_DELETE_ON_START_ENV_VAR),
        _context->dirs_delete_on_start);

    _context->dirs_delete_on_shutdown = aeron_config_parse_bool(
        getenv(AERON_DIR_DELETE_ON_SHUTDOWN_ENV_VAR),
        _context->dirs_delete_on_shutdown);

    _context->warn_if_dirs_exist = aeron_config_parse_bool(
        getenv(AERON_DIR_WARN_IF_EXISTS_ENV_VAR),
        _context->warn_if_dirs_exist);

    _context->term_buffer_sparse_file = aeron_config_parse_bool(
        getenv(AERON_TERM_BUFFER_SPARSE_FILE_ENV_VAR),
        _context->term_buffer_sparse_file);

    _context->perform_storage_checks = aeron_config_parse_bool(
        getenv(AERON_PERFORM_STORAGE_CHECKS_ENV_VAR),
        _context->perform_storage_checks);

    _context->spies_simulate_connection = aeron_config_parse_bool(
        getenv(AERON_SPIES_SIMULATE_CONNECTION_ENV_VAR),
        _context->spies_simulate_connection);

    _context->print_configuration_on_start = aeron_config_parse_bool(
        getenv(AERON_PRINT_CONFIGURATION_ON_START_ENV_VAR),
        _context->print_configuration_on_start);

    _context->reliable_stream = aeron_config_parse_bool(
        getenv(AERON_RELIABLE_STREAM_ENV_VAR),
        _context->reliable_stream);

    _context->tether_subscriptions = aeron_config_parse_bool(
        getenv(AERON_TETHER_SUBSCRIPTIONS_ENV_VAR),
        _context->tether_subscriptions);

    _context->rejoin_stream = aeron_config_parse_bool(
        getenv(AERON_REJOIN_STREAM_ENV_VAR),
        _context->rejoin_stream);

    _context->to_driver_buffer_length = aeron_config_parse_size64(
        AERON_TO_CONDUCTOR_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_TO_CONDUCTOR_BUFFER_LENGTH_ENV_VAR),
        _context->to_driver_buffer_length,
        1024 + AERON_RB_TRAILER_LENGTH,
        INT32_MAX);

    _context->to_clients_buffer_length = aeron_config_parse_size64(
        AERON_TO_CLIENTS_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_TO_CLIENTS_BUFFER_LENGTH_ENV_VAR),
        _context->to_clients_buffer_length,
        1024 + AERON_BROADCAST_BUFFER_TRAILER_LENGTH,
        INT32_MAX);

    _context->counters_values_buffer_length = aeron_config_parse_size64(
        AERON_COUNTERS_VALUES_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_COUNTERS_VALUES_BUFFER_LENGTH_ENV_VAR),
        _context->counters_values_buffer_length,
        1024,
        INT32_MAX);

    _context->error_buffer_length = aeron_config_parse_size64(
        AERON_ERROR_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_ERROR_BUFFER_LENGTH_ENV_VAR),
        _context->error_buffer_length,
        1024,
        INT32_MAX);

    _context->client_liveness_timeout_ns = aeron_config_parse_duration_ns(
        AERON_CLIENT_LIVENESS_TIMEOUT_ENV_VAR,
        getenv(AERON_CLIENT_LIVENESS_TIMEOUT_ENV_VAR),
        _context->client_liveness_timeout_ns,
        1000,
        INT64_MAX);

    _context->publication_linger_timeout_ns = aeron_config_parse_duration_ns(
        AERON_PUBLICATION_LINGER_TIMEOUT_ENV_VAR,
        getenv(AERON_PUBLICATION_LINGER_TIMEOUT_ENV_VAR),
        _context->publication_linger_timeout_ns,
        1000,
        INT64_MAX);

    _context->term_buffer_length = aeron_config_parse_size64(
        AERON_TERM_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_TERM_BUFFER_LENGTH_ENV_VAR),
        _context->term_buffer_length,
        1024,
        INT32_MAX);

    _context->ipc_term_buffer_length = aeron_config_parse_size64(
        AERON_IPC_TERM_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_IPC_TERM_BUFFER_LENGTH_ENV_VAR),
        _context->ipc_term_buffer_length,
        1024,
        INT32_MAX);

    _context->mtu_length = aeron_config_parse_size64(
        AERON_MTU_LENGTH_ENV_VAR,
        getenv(AERON_MTU_LENGTH_ENV_VAR),
        _context->mtu_length,
        AERON_DATA_HEADER_LENGTH,
        AERON_MAX_UDP_PAYLOAD_LENGTH);

    _context->ipc_mtu_length = aeron_config_parse_size64(
        AERON_IPC_MTU_LENGTH_ENV_VAR,
        getenv(AERON_IPC_MTU_LENGTH_ENV_VAR),
        _context->ipc_mtu_length,
        AERON_DATA_HEADER_LENGTH,
        AERON_MAX_UDP_PAYLOAD_LENGTH);

    _context->ipc_publication_window_length = aeron_config_parse_size64(
        AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR,
        getenv(AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR),
        _context->ipc_publication_window_length,
        0,
        INT32_MAX);

    _context->publication_window_length = aeron_config_parse_size64(
        AERON_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR,
        getenv(AERON_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR),
        _context->publication_window_length,
        0,
        INT32_MAX);

    _context->socket_rcvbuf = aeron_config_parse_size64(
        AERON_SOCKET_SO_RCVBUF_ENV_VAR,
        getenv(AERON_SOCKET_SO_RCVBUF_ENV_VAR),
        _context->socket_rcvbuf,
        0,
        INT32_MAX);

    _context->socket_sndbuf = aeron_config_parse_size64(
        AERON_SOCKET_SO_SNDBUF_ENV_VAR,
        getenv(AERON_SOCKET_SO_SNDBUF_ENV_VAR),
        _context->socket_sndbuf,
        0,
        INT32_MAX);

    _context->multicast_ttl = (uint8_t)aeron_config_parse_uint64(
        AERON_SOCKET_MULTICAST_TTL_ENV_VAR,
        getenv(AERON_SOCKET_MULTICAST_TTL_ENV_VAR),
        _context->multicast_ttl,
        0,
        255);

    _context->send_to_sm_poll_ratio = (uint8_t)aeron_config_parse_uint64(
        AERON_SEND_TO_STATUS_POLL_RATIO_ENV_VAR,
        getenv(AERON_SEND_TO_STATUS_POLL_RATIO_ENV_VAR),
        _context->send_to_sm_poll_ratio,
        1,
        INT32_MAX);

    _context->driver_timeout_ms = aeron_config_parse_uint64(
        AERON_DRIVER_TIMEOUT_ENV_VAR,
        getenv(AERON_DRIVER_TIMEOUT_ENV_VAR),
        _context->driver_timeout_ms,
        0,
        INT64_MAX);

    _context->status_message_timeout_ns = aeron_config_parse_duration_ns(
        AERON_RCV_STATUS_MESSAGE_TIMEOUT_ENV_VAR,
        getenv(AERON_RCV_STATUS_MESSAGE_TIMEOUT_ENV_VAR),
        _context->status_message_timeout_ns,
        1000,
        INT64_MAX);

    _context->image_liveness_timeout_ns = aeron_config_parse_duration_ns(
        AERON_IMAGE_LIVENESS_TIMEOUT_ENV_VAR,
        getenv(AERON_IMAGE_LIVENESS_TIMEOUT_ENV_VAR),
        _context->image_liveness_timeout_ns,
        1000,
        INT64_MAX);

    _context->initial_window_length = aeron_config_parse_size64(
        AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR,
        getenv(AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR),
        _context->initial_window_length,
        256,
        INT32_MAX);

    _context->loss_report_length = aeron_config_parse_size64(
        AERON_LOSS_REPORT_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_LOSS_REPORT_BUFFER_LENGTH_ENV_VAR),
        _context->loss_report_length,
        1024,
        INT32_MAX);

    _context->file_page_size = aeron_config_parse_size64(
        AERON_FILE_PAGE_SIZE_ENV_VAR,
        getenv(AERON_FILE_PAGE_SIZE_ENV_VAR),
        _context->file_page_size,
        4 * 1024,
        INT32_MAX);

    _context->publication_unblock_timeout_ns = aeron_config_parse_duration_ns(
        AERON_PUBLICATION_UNBLOCK_TIMEOUT_ENV_VAR,
        getenv(AERON_PUBLICATION_UNBLOCK_TIMEOUT_ENV_VAR),
        _context->publication_unblock_timeout_ns,
        1000,
        INT64_MAX);

    _context->publication_connection_timeout_ns = aeron_config_parse_duration_ns(
        AERON_PUBLICATION_CONNECTION_TIMEOUT_ENV_VAR,
        getenv(AERON_PUBLICATION_CONNECTION_TIMEOUT_ENV_VAR),
        _context->publication_connection_timeout_ns,
        1000,
        INT64_MAX);

    _context->timer_interval_ns = aeron_config_parse_duration_ns(
        AERON_TIMER_INTERVAL_ENV_VAR,
        getenv(AERON_TIMER_INTERVAL_ENV_VAR),
        _context->timer_interval_ns,
        1000,
        INT64_MAX);

    _context->counter_free_to_reuse_ns = aeron_config_parse_duration_ns(
        AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_ENV_VAR,
        getenv(AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_ENV_VAR),
        _context->counter_free_to_reuse_ns,
        0,
        INT64_MAX);

    _context->untethered_window_limit_timeout_ns = aeron_config_parse_duration_ns(
        AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_ENV_VAR,
        getenv(AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_ENV_VAR),
        _context->untethered_window_limit_timeout_ns,
        1000,
        INT64_MAX);

    _context->untethered_resting_timeout_ns = aeron_config_parse_duration_ns(
        AERON_UNTETHERED_RESTING_TIMEOUT_ENV_VAR,
        getenv(AERON_UNTETHERED_RESTING_TIMEOUT_ENV_VAR),
        _context->untethered_resting_timeout_ns,
        1000,
        INT64_MAX);

    _context->retransmit_unicast_delay_ns = aeron_config_parse_duration_ns(
        AERON_RETRANSMIT_UNICAST_DELAY_ENV_VAR,
        getenv(AERON_RETRANSMIT_UNICAST_DELAY_ENV_VAR),
        _context->retransmit_unicast_delay_ns,
        0,
        INT64_MAX);

    _context->retransmit_unicast_linger_ns = aeron_config_parse_duration_ns(
        AERON_RETRANSMIT_UNICAST_LINGER_ENV_VAR,
        getenv(AERON_RETRANSMIT_UNICAST_LINGER_ENV_VAR),
        _context->retransmit_unicast_linger_ns,
        1000,
        INT64_MAX);

    _context->nak_multicast_group_size = (size_t)aeron_config_parse_uint64(
        AERON_NAK_MULTICAST_GROUP_SIZE_ENV_VAR,
        getenv(AERON_NAK_MULTICAST_GROUP_SIZE_ENV_VAR),
        _context->nak_multicast_group_size,
        1,
        INT32_MAX);

    _context->nak_multicast_max_backoff_ns = aeron_config_parse_duration_ns(
        AERON_NAK_MULTICAST_MAX_BACKOFF_ENV_VAR,
        getenv(AERON_NAK_MULTICAST_MAX_BACKOFF_ENV_VAR),
        _context->nak_multicast_max_backoff_ns,
        1000,
        INT64_MAX);

    _context->nak_unicast_delay_ns = aeron_config_parse_duration_ns(
        AERON_NAK_UNICAST_DELAY_ENV_VAR,
        getenv(AERON_NAK_UNICAST_DELAY_ENV_VAR),
        _context->nak_unicast_delay_ns,
        1000,
        INT64_MAX);

    _context->publication_reserved_session_id_low = aeron_config_parse_int32(
        AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_ENV_VAR,
        getenv(AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_ENV_VAR),
        _context->publication_reserved_session_id_low,
        INT32_MIN,
        INT32_MAX);

    _context->publication_reserved_session_id_high = aeron_config_parse_int32(
        AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_ENV_VAR,
        getenv(AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_ENV_VAR),
        _context->publication_reserved_session_id_high,
        INT32_MIN,
        INT32_MAX);

    _context->to_driver_buffer = NULL;
    _context->to_clients_buffer = NULL;
    _context->counters_values_buffer = NULL;
    _context->counters_metadata_buffer = NULL;
    _context->error_buffer = NULL;

    _context->nano_clock = aeron_nano_clock;
    _context->epoch_clock = aeron_epoch_clock;
    if (aeron_clock_cache_alloc(&_context->cached_clock) < 0)
    {
        return -1;
    }

    _context->conductor_idle_strategy_name = aeron_strndup("backoff", AERON_MAX_PATH);
    _context->shared_idle_strategy_name = aeron_strndup("backoff", AERON_MAX_PATH);
    _context->shared_network_idle_strategy_name = aeron_strndup("backoff", AERON_MAX_PATH);
    _context->sender_idle_strategy_name = aeron_strndup("backoff", AERON_MAX_PATH);
    _context->receiver_idle_strategy_name = aeron_strndup("backoff", AERON_MAX_PATH);

    _context->conductor_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_CONDUCTOR_IDLE_STRATEGY_INIT_ARGS_ENV_VAR);
    if ((_context->conductor_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->conductor_idle_strategy_state,
        AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR,
        _context->conductor_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->shared_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_SHARED_IDLE_STRATEGY_ENV_INIT_ARGS_VAR);
    if ((_context->shared_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SHARED_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->shared_idle_strategy_state,
        AERON_SHARED_IDLE_STRATEGY_ENV_VAR,
        _context->shared_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->shared_network_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_SHAREDNETWORK_IDLE_STRATEGY_INIT_ARGS_ENV_VAR);
    if ((_context->shared_network_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->shared_network_idle_strategy_state,
        AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR,
        _context->shared_network_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->sender_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_SENDER_IDLE_STRATEGY_INIT_ARGS_ENV_VAR);
    if ((_context->sender_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SENDER_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->sender_idle_strategy_state,
        AERON_SENDER_IDLE_STRATEGY_ENV_VAR,
        _context->sender_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->receiver_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_RECEIVER_IDLE_STRATEGY_INIT_ARGS_ENV_VAR);
    if ((_context->receiver_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->receiver_idle_strategy_state,
        AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR,
        _context->receiver_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->usable_fs_space_func = _context->perform_storage_checks ?
        aeron_usable_fs_space : aeron_usable_fs_space_disabled;
    _context->map_raw_log_func = aeron_map_raw_log;
    _context->map_raw_log_close_func = aeron_map_raw_log_close;

    _context->to_driver_interceptor_func = aeron_driver_conductor_to_driver_interceptor_null;
    _context->to_client_interceptor_func = aeron_driver_conductor_to_client_interceptor_null;

    if ((_context->termination_validator_func = aeron_driver_termination_validator_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_DRIVER_TERMINATION_VALIDATOR_ENV_VAR, "deny"))) == NULL)
    {
        return -1;
    }

    _context->termination_validator_state = NULL;

    _context->termination_hook_func = NULL;
    _context->termination_hook_state = NULL;

    if ((_context->udp_channel_transport_bindings = aeron_udp_channel_transport_bindings_load_media(
        AERON_CONFIG_GETENV_OR_DEFAULT(
                AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_ENV_VAR,
                AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT))) == NULL)
    {
        return -1;
    }

    if ((value = getenv(AERON_UDP_CHANNEL_OUTGOING_INTERCEPTORS_ENV_VAR)))
    {
        if ((_context->udp_channel_outgoing_interceptor_bindings = aeron_udp_channel_interceptor_bindings_load(
            NULL, value)) == NULL)
        {
            return -1;
        }
    }

    if ((value = getenv(AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR)))
    {
        if ((_context->udp_channel_incoming_interceptor_bindings = aeron_udp_channel_interceptor_bindings_load(
            NULL, value)) == NULL)
        {
            return -1;
        }
    }

    if (getenv(AERON_AGENT_MASK_ENV_VAR))
    {
        if (aeron_driver_agent_context_init(_context) < 0)
        {
            return -1;
        }
    }

#ifdef HAVE_UUID_GENERATE
    uuid_t id;
    uuid_generate(id);

    struct uuid_as_uint64
    {
        uint64_t high;
        uint64_t low;
    }

    *id_as_uint64 = (struct uuid_as_uint64 *)&id;
    _context->next_receiver_id = id_as_uint64->high ^ id_as_uint64->low;
#else
    /* pure random id */
    _context->next_receiver_id = aeron_randomised_int32();
#endif

    *context = _context;
    return 0;
}

int aeron_driver_context_close(aeron_driver_context_t *context)
{
    if (NULL == context)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_driver_context_close(NULL): %s", strerror(EINVAL));
        return -1;
    }

    aeron_mpsc_concurrent_array_queue_close(&context->conductor_command_queue);
    aeron_spsc_concurrent_array_queue_close(&context->sender_command_queue);
    aeron_spsc_concurrent_array_queue_close(&context->receiver_command_queue);

    aeron_unmap(&context->cnc_map);
    aeron_unmap(&context->loss_report);

    aeron_free((void *)context->aeron_dir);
    aeron_free(context->conductor_idle_strategy_state);
    aeron_free(context->receiver_idle_strategy_state);
    aeron_free(context->sender_idle_strategy_state);
    aeron_free(context->shared_idle_strategy_state);
    aeron_free(context->shared_network_idle_strategy_state);
    aeron_free((void *)context->conductor_idle_strategy_name);
    aeron_free((void *)context->shared_network_idle_strategy_name);
    aeron_free((void *)context->shared_idle_strategy_name);
    aeron_free((void *)context->sender_idle_strategy_name);
    aeron_free((void *)context->receiver_idle_strategy_name);
    aeron_free((void *)context->conductor_idle_strategy_init_args);
    aeron_free((void *)context->sender_idle_strategy_init_args);
    aeron_free((void *)context->receiver_idle_strategy_init_args);
    aeron_free((void *)context->shared_idle_strategy_init_args);
    aeron_free((void *)context->shared_network_idle_strategy_init_args);
    aeron_clock_cache_free(context->cached_clock);
    aeron_free(context);

    return 0;
}

int aeron_driver_context_validate_mtu_length(uint64_t mtu_length)
{
    if (mtu_length < AERON_DATA_HEADER_LENGTH || mtu_length > AERON_MAX_UDP_PAYLOAD_LENGTH)
    {
        aeron_set_err(
            EINVAL,
            "mtuLength must be a >= HEADER_LENGTH and <= MAX_UDP_PAYLOAD_LENGTH: mtuLength=%" PRIu64,
            mtu_length);
        return -1;
    }

    if ((mtu_length & (AERON_LOGBUFFER_FRAME_ALIGNMENT - 1)) != 0)
    {
        aeron_set_err(EINVAL, "mtuLength must be a multiple of FRAME_ALIGNMENT: mtuLength=%" PRIu64, mtu_length);
        return -1;
    }

    return 0;
}

bool aeron_is_driver_active_with_cnc(
    aeron_mapped_file_t *cnc_mmap, int64_t timeout_ms, int64_t now_ms, aeron_log_func_t log_func)
{
    char buffer[AERON_MAX_PATH];
    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)cnc_mmap->addr;
    int32_t cnc_version;

    while (0 == (cnc_version = aeron_cnc_version_volatile(metadata)))
    {
        if (aeron_epoch_clock() > (now_ms + timeout_ms))
        {
            snprintf(buffer, sizeof(buffer) - 1, "ERROR: aeron cnc file version was 0 for timeout");
            return false;
        }

        aeron_micro_sleep(1000);
    }

    if (aeron_semantic_version_major(AERON_CNC_VERSION) != aeron_semantic_version_major(cnc_version))
    {
        snprintf(
            buffer, sizeof(buffer) - 1,
            "ERROR: aeron cnc version not compatible: app version=%d.%d.%d file=%d.%d.%d",
            (int)aeron_semantic_version_major(AERON_CNC_VERSION),
            (int)aeron_semantic_version_minor(AERON_CNC_VERSION),
            (int)aeron_semantic_version_patch(AERON_CNC_VERSION),
            (int)aeron_semantic_version_major(cnc_version),
            (int)aeron_semantic_version_minor(cnc_version),
            (int)aeron_semantic_version_patch(cnc_version));

        log_func(buffer);
    }
    else
    {
        aeron_mpsc_rb_t rb;

        if (aeron_mpsc_rb_init(
            &rb, aeron_cnc_to_driver_buffer(metadata), (size_t)metadata->to_driver_buffer_length) != 0)
        {
            snprintf(buffer, sizeof(buffer) - 1, "ERROR: aeron cnc file could not init to-driver buffer");
            log_func(buffer);
        }
        else
        {
            int64_t timestamp_ms = aeron_mpsc_rb_consumer_heartbeat_time_value(&rb);
            int64_t diff = now_ms - timestamp_ms;

            snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron driver heartbeat is %" PRId64 " ms old", diff);
            log_func(buffer);

            if (diff <= timeout_ms)
            {
                return true;
            }
        }
    }

    return false;
}

bool aeron_is_driver_active(const char *dirname, int64_t timeout_ms, aeron_log_func_t log_func)
{
    char buffer[AERON_MAX_PATH];
    bool result = false;

    if (aeron_is_directory(dirname))
    {
        aeron_mapped_file_t cnc_map = { NULL, 0 };

        snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron directory %s exists", dirname);
        log_func(buffer);

        snprintf(buffer, sizeof(buffer) - 1, "%s/%s", dirname, AERON_CNC_FILE);
        if (aeron_map_existing_file(&cnc_map, buffer) < 0)
        {
            snprintf(buffer, sizeof(buffer) - 1, "INFO: failed to mmap CnC file");
            log_func(buffer);
            return false;
        }

        snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron CnC file %s/%s exists", dirname, AERON_CNC_FILE);
        log_func(buffer);

        result = aeron_is_driver_active_with_cnc(&cnc_map, timeout_ms, aeron_epoch_clock(), log_func);

        aeron_unmap(&cnc_map);
    }

    return result;
}

size_t aeron_cnc_length(aeron_driver_context_t *context)
{
    return aeron_cnc_computed_length(
        context->to_driver_buffer_length +
        context->to_clients_buffer_length +
        AERON_COUNTERS_METADATA_BUFFER_LENGTH(context->counters_values_buffer_length) +
        context->counters_values_buffer_length +
        context->error_buffer_length,
        context->file_page_size);
}

extern int32_t aeron_cnc_version_volatile(aeron_cnc_metadata_t *metadata);

extern void aeron_cnc_version_signal_cnc_ready(aeron_cnc_metadata_t *metadata, int32_t cnc_version);

extern uint8_t *aeron_cnc_to_driver_buffer(aeron_cnc_metadata_t *metadata);

extern uint8_t *aeron_cnc_to_clients_buffer(aeron_cnc_metadata_t *metadata);

extern uint8_t *aeron_cnc_counters_metadata_buffer(aeron_cnc_metadata_t *metadata);

extern uint8_t *aeron_cnc_counters_values_buffer(aeron_cnc_metadata_t *metadata);

extern uint8_t *aeron_cnc_error_log_buffer(aeron_cnc_metadata_t *metadata);

extern size_t aeron_cnc_computed_length(size_t total_length_of_buffers, size_t alignment);

extern size_t aeron_producer_window_length(size_t producer_window_length, size_t term_length);


#define AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(r, a) \
do \
{ \
    if (NULL == (a)) \
    { \
        aeron_set_err(EINVAL, "%s", strerror(EINVAL)); \
        return (r); \
    } \
} \
while (false)

int aeron_driver_context_set_dir(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    snprintf(context->aeron_dir, AERON_MAX_PATH - 1, "%s", value);
    return 0;
}

const char *aeron_driver_context_get_dir(aeron_driver_context_t *context)
{
    return NULL != context ? context->aeron_dir : NULL;
}

int aeron_driver_context_set_dir_warn_if_exists(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->warn_if_dirs_exist = value;
    return 0;
}

bool aeron_driver_context_get_dir_warn_if_exists(aeron_driver_context_t *context)
{
    return NULL != context ? context->warn_if_dirs_exist : AERON_DIR_WARN_IF_EXISTS_DEFAULT;
}

int aeron_driver_context_set_threading_mode(aeron_driver_context_t *context, aeron_threading_mode_t mode)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->threading_mode = mode;
    return 0;
}

aeron_threading_mode_t aeron_driver_context_get_threading_mode(aeron_driver_context_t *context)
{
    return NULL != context ? context->threading_mode : AERON_THREADING_MODE_DEFAULT;
}

int aeron_driver_context_set_dir_delete_on_start(aeron_driver_context_t * context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->dirs_delete_on_start = value;
    return 0;
}

bool aeron_driver_context_get_dir_delete_on_start(aeron_driver_context_t *context)
{
    return NULL != context ? context->dirs_delete_on_start : AERON_DIR_DELETE_ON_START_DEFAULT;
}

int aeron_driver_context_set_dir_delete_on_shutdown(aeron_driver_context_t * context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->dirs_delete_on_shutdown = value;
    return 0;
}

bool aeron_driver_context_get_dir_delete_on_shutdown(aeron_driver_context_t *context)
{
    return NULL != context ? context->dirs_delete_on_shutdown : AERON_DIR_DELETE_ON_SHUTDOWN_DEFAULT;
}

int aeron_driver_context_set_to_conductor_buffer_length(aeron_driver_context_t *context, size_t length)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->to_driver_buffer_length = length;
    return 0;
}

size_t aeron_driver_context_get_to_conductor_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->to_driver_buffer_length : AERON_TO_CONDUCTOR_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_to_clients_buffer_length(aeron_driver_context_t *context, size_t length)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->to_clients_buffer_length = length;
    return 0;
}

size_t aeron_driver_context_get_to_clients_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->to_clients_buffer_length : AERON_TO_CLIENTS_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_counters_buffer_length(aeron_driver_context_t *context, size_t length)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->counters_values_buffer_length = length;
    return 0;
}

size_t aeron_driver_context_get_counters_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->counters_values_buffer_length : AERON_COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_error_buffer_length(aeron_driver_context_t *context, size_t length)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->error_buffer_length = length;
    return 0;
}

size_t aeron_driver_context_get_error_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->error_buffer_length : AERON_ERROR_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_client_liveness_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->client_liveness_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_client_liveness_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->client_liveness_timeout_ns : AERON_CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_term_buffer_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->term_buffer_length = value;
    return 0;
}

size_t aeron_driver_context_get_term_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->term_buffer_length : AERON_TERM_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_ipc_term_buffer_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->ipc_term_buffer_length = value;
    return 0;
}

size_t aeron_driver_context_get_ipc_term_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->ipc_term_buffer_length : AERON_IPC_TERM_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_term_buffer_sparse_file(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->term_buffer_sparse_file = value;
    return 0;
}

bool aeron_driver_context_get_term_buffer_sparse_file(aeron_driver_context_t *context)
{
    return NULL != context ? context->term_buffer_sparse_file : AERON_TERM_BUFFER_SPARSE_FILE_DEFAULT;
}

int aeron_driver_context_set_perform_storage_checks(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->perform_storage_checks = value;
    return 0;
}

bool aeron_driver_context_get_perform_storage_checks(aeron_driver_context_t *context)
{
    return NULL != context ? context->perform_storage_checks : AERON_PERFORM_STORAGE_CHECKS_DEFAULT;
}

int aeron_driver_context_set_spies_simulate_connection(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->spies_simulate_connection = value;
    return 0;
}

bool aeron_driver_context_get_spies_simulate_connection(aeron_driver_context_t *context)
{
    return NULL != context ? context->spies_simulate_connection : AERON_SPIES_SIMULATE_CONNECTION_DEFAULT;
}

int aeron_driver_context_set_file_page_size(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->file_page_size = value;
    return 0;
}

size_t aeron_driver_context_get_file_page_size(aeron_driver_context_t *context)
{
    return NULL != context ? context->file_page_size : AERON_FILE_PAGE_SIZE_DEFAULT;
}

int aeron_driver_context_set_mtu_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->mtu_length = value;
    return 0;
}

size_t aeron_driver_context_get_mtu_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->mtu_length : AERON_MTU_LENGTH_DEFAULT;
}

int aeron_driver_context_set_ipc_mtu_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->ipc_mtu_length = value;
    return 0;
}

size_t aeron_driver_context_get_ipc_mtu_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->ipc_mtu_length : AERON_IPC_MTU_LENGTH_DEFAULT;
}

int aeron_driver_context_set_ipc_publication_term_window_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->ipc_publication_window_length = value;
    return 0;
}

size_t aeron_driver_context_get_ipc_publication_term_window_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->ipc_publication_window_length : AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT;
}

int aeron_driver_context_set_publication_term_window_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_window_length = value;
    return 0;
}

size_t aeron_driver_context_get_publication_term_window_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->publication_window_length : AERON_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT;
}

int aeron_driver_context_set_publication_linger_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_linger_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_publication_linger_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->publication_linger_timeout_ns : AERON_PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_socket_so_rcvbuf(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->socket_rcvbuf = value;
    return 0;
}

size_t aeron_driver_context_get_socket_so_rcvbuf(aeron_driver_context_t *context)
{
    return NULL != context ? context->socket_rcvbuf : AERON_SOCKET_SO_RCVBUF_DEFAULT;
}

int aeron_driver_context_set_socket_so_sndbuf(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->socket_sndbuf = value;
    return 0;
}

size_t aeron_driver_context_get_socket_so_sndbuf(aeron_driver_context_t *context)
{
    return NULL != context ? context->socket_sndbuf : AERON_SOCKET_SO_SNDBUF_DEFAULT;
}

int aeron_driver_context_set_socket_multicast_ttl(aeron_driver_context_t *context, uint8_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->multicast_ttl = value;
    return 0;
}

uint8_t aeron_driver_context_get_socket_multicast_ttl(aeron_driver_context_t *context)
{
    return NULL != context ? context->multicast_ttl : AERON_SOCKET_MULTICAST_TTL_DEFAULT;
}

int aeron_driver_context_set_send_to_status_poll_ratio(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->send_to_sm_poll_ratio = value;
    return 0;
}

size_t aeron_driver_context_get_send_to_status_poll_ratio(aeron_driver_context_t *context)
{
    return NULL != context ? context->send_to_sm_poll_ratio : AERON_SEND_TO_STATUS_POLL_RATIO_DEFAULT;
}

int aeron_driver_context_set_rcv_status_message_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->status_message_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_rcv_status_message_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->status_message_timeout_ns : AERON_RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_multicast_flowcontrol_supplier(
    aeron_driver_context_t *context, aeron_flow_control_strategy_supplier_func_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    context->multicast_flow_control_supplier_func = value;
    return 0;
}

aeron_flow_control_strategy_supplier_func_t aeron_driver_context_get_multicast_flowcontrol_supplier(
    aeron_driver_context_t *context)
{
    return NULL != context ?
        context->multicast_flow_control_supplier_func :
        aeron_flow_control_strategy_supplier_load(AERON_MULTICAST_FLOWCONTROL_SUPPLIER_DEFAULT);
}

int aeron_driver_context_set_unicast_flowcontrol_supplier(
    aeron_driver_context_t *context, aeron_flow_control_strategy_supplier_func_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    context->unicast_flow_control_supplier_func = value;
    return 0;
}

aeron_flow_control_strategy_supplier_func_t aeron_driver_context_get_unicast_flowcontrol_supplier(
    aeron_driver_context_t *context)
{
    return NULL != context ?
        context->unicast_flow_control_supplier_func :
        aeron_flow_control_strategy_supplier_load(AERON_UNICAST_FLOWCONTROL_SUPPLIER_DEFAULT);
}

int aeron_driver_context_set_image_liveness_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->image_liveness_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_image_liveness_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->image_liveness_timeout_ns : AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_rcv_initial_window_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->initial_window_length = value;
    return 0;
}

size_t aeron_driver_context_get_rcv_initial_window_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->initial_window_length : AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT;
}

int aeron_driver_context_set_congestioncontrol_supplier(
    aeron_driver_context_t *context, aeron_congestion_control_strategy_supplier_func_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    context->congestion_control_supplier_func = value;
    return 0;
}

aeron_congestion_control_strategy_supplier_func_t aeron_driver_context_get_congestioncontrol_supplier(
    aeron_driver_context_t *context)
{
    return NULL != context ? context->congestion_control_supplier_func :
    aeron_congestion_control_strategy_supplier_load(AERON_CONGESTIONCONTROL_SUPPLIER_DEFAULT);
}

int aeron_driver_context_set_loss_report_buffer_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->loss_report_length = value;
    return 0;
}

size_t aeron_driver_context_get_loss_report_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->loss_report_length : AERON_LOSS_REPORT_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_publication_unblock_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_unblock_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_publication_unblock_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->publication_unblock_timeout_ns : AERON_PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_publication_connection_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_connection_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_publication_connection_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->publication_connection_timeout_ns : AERON_PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_timer_interval_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->timer_interval_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_timer_interval_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->timer_interval_ns : AERON_TIMER_INTERVAL_NS_DEFAULT;
}

int aeron_driver_context_set_sender_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->sender_idle_strategy_state);
    aeron_free((void *)context->sender_idle_strategy_name);

    if ((context->sender_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->sender_idle_strategy_state,
        AERON_SENDER_IDLE_STRATEGY_ENV_VAR,
        context->sender_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->sender_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);
    return 0;
}

const char *aeron_driver_context_get_sender_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->sender_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_conductor_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->conductor_idle_strategy_state);
    aeron_free((void *)context->conductor_idle_strategy_name);

    if ((context->conductor_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->conductor_idle_strategy_state,
        AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR,
        context->conductor_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->conductor_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);
    return 0;
}

const char *aeron_driver_context_get_conductor_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->conductor_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_receiver_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->receiver_idle_strategy_state);
    aeron_free((void *)context->receiver_idle_strategy_name);

    if ((context->receiver_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->receiver_idle_strategy_state,
        AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR,
        context->receiver_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->receiver_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);
    return 0;
}

const char *aeron_driver_context_get_receiver_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_sharednetwork_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->shared_network_idle_strategy_state);
    aeron_free((void *)context->shared_network_idle_strategy_name);

    if ((context->shared_network_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->shared_network_idle_strategy_state,
        AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR,
        context->shared_network_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->shared_network_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);
    return 0;
}

const char *aeron_driver_context_get_sharednetwork_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->shared_network_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_shared_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->shared_idle_strategy_state);
    aeron_free((void *)context->shared_idle_strategy_name);

    if ((context->shared_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->shared_idle_strategy_state,
        AERON_SHARED_IDLE_STRATEGY_ENV_VAR,
        context->shared_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->shared_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);
    return 0;
}

const char *aeron_driver_context_get_shared_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->shared_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_sender_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free((void *)context->sender_idle_strategy_init_args);
    context->sender_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_sender_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->sender_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_conductor_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free((void *)context->conductor_idle_strategy_init_args);
    context->conductor_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_conductor_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->conductor_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_receiver_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free((void *)context->receiver_idle_strategy_init_args);
    context->receiver_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_receiver_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_sharednetwork_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free((void *)context->shared_network_idle_strategy_init_args);
    context->shared_network_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_sharednetwork_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->shared_network_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_shared_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free((void *)context->shared_idle_strategy_init_args);
    context->shared_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_shared_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->shared_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_agent_on_start_function(
    aeron_driver_context_t *context, aeron_agent_on_start_func_t value, void *state)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->agent_on_start_func = value;
    context->agent_on_start_state = state;
    return 0;
}

aeron_agent_on_start_func_t aeron_driver_context_get_agent_on_start_function(aeron_driver_context_t *context)
{
    return NULL != context ? context->agent_on_start_func : NULL;
}

void *aeron_driver_context_get_agent_on_start_state(aeron_driver_context_t *context)
{
    return NULL != context ? context->agent_on_start_state : NULL;
}

int aeron_driver_context_set_counters_free_to_reuse_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->counter_free_to_reuse_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_counters_free_to_reuse_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->counter_free_to_reuse_ns : AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_driver_termination_validator(
    aeron_driver_context_t *context, aeron_driver_termination_validator_func_t value, void *state)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->termination_validator_func = value;
    context->termination_validator_state = state;
    return 0;
}

aeron_driver_termination_validator_func_t aeron_driver_context_get_driver_termination_validator(
    aeron_driver_context_t *context)
{
    return NULL != context ? context->termination_validator_func : aeron_driver_termination_validator_default_deny;
}

void *aeron_driver_context_get_driver_termination_validator_state(aeron_driver_context_t *context)
{
    return NULL != context ? context->termination_validator_state : NULL;
}

int aeron_driver_context_set_driver_termination_hook(
    aeron_driver_context_t *context, aeron_driver_termination_hook_func_t value, void *state)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->termination_hook_func = value;
    context->termination_hook_state = state;
    return 0;
}

aeron_driver_termination_hook_func_t aeron_driver_context_get_driver_termination_hook(aeron_driver_context_t *context)
{
    return NULL != context ? context->termination_hook_func : NULL;
}

void *aeron_driver_context_get_driver_termination_hook_state(aeron_driver_context_t *context)
{
    return NULL != context ? context->termination_hook_state : NULL;
}

int aeron_driver_context_set_print_configuration(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->print_configuration_on_start = value;
    return 0;
}

bool aeron_driver_context_get_print_configuration(aeron_driver_context_t *context)
{
    return NULL != context ? context->print_configuration_on_start : AERON_PRINT_CONFIGURATION_DEFAULT;
}

int aeron_driver_context_set_reliable_stream(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->reliable_stream = value;
    return 0;
}

bool aeron_driver_context_get_reliable_stream(aeron_driver_context_t *context)
{
    return NULL != context ? context->reliable_stream : AERON_RELIABLE_STREAM_DEFAULT;
}

int aeron_driver_context_set_tether_subscriptions(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->tether_subscriptions = value;
    return 0;
}

bool aeron_driver_context_get_tether_subscriptions(aeron_driver_context_t *context)
{
    return NULL != context ? context->tether_subscriptions : AERON_TETHER_SUBSCRIPTIONS_DEFAULT;
}

int aeron_driver_context_set_untethered_window_limit_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->untethered_window_limit_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_untethered_window_limit_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->untethered_window_limit_timeout_ns : AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_untethered_resting_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->untethered_resting_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_untethered_resting_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->untethered_resting_timeout_ns : AERON_UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_driver_timeout_ms(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->driver_timeout_ms = value;
    return 0;
}

uint64_t aeron_driver_context_get_driver_timeout_ms(aeron_driver_context_t *context)
{
    return NULL != context ? context->driver_timeout_ms : AERON_DRIVER_TIMEOUT_MS_DEFAULT;
}

int aeron_driver_context_set_retransmit_unicast_delay_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->retransmit_unicast_delay_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_retransmit_unicast_delay_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->retransmit_unicast_delay_ns : AERON_RETRANSMIT_UNICAST_DELAY_NS_DEFAULT;
}

int aeron_driver_context_set_retransmit_unicast_linger_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->retransmit_unicast_linger_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_retransmit_unicast_linger_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->retransmit_unicast_linger_ns : AERON_RETRANSMIT_UNICAST_LINGER_NS_DEFAULT;
}

int aeron_driver_context_set_nak_multicast_group_size(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->nak_multicast_group_size = value;
    return 0;
}

size_t aeron_driver_context_get_nak_multicast_group_size(aeron_driver_context_t *context)
{
    return NULL != context ? context->nak_multicast_group_size : AERON_NAK_MULTICAST_GROUP_SIZE_DEFAULT;
}

int aeron_driver_context_set_nak_multicast_max_backoff_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->nak_multicast_max_backoff_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_nak_multicast_max_backoff_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->nak_multicast_max_backoff_ns : AERON_NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT;
}

int aeron_driver_context_set_nak_unicast_delay_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->nak_unicast_delay_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_nak_unicast_delay_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->nak_unicast_delay_ns : AERON_NAK_UNICAST_DELAY_NS_DEFAULT;
}

int aeron_driver_context_set_udp_channel_transport_bindings(
    aeron_driver_context_t *context, aeron_udp_channel_transport_bindings_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->udp_channel_transport_bindings = value;
    return 0;
}

aeron_udp_channel_transport_bindings_t *aeron_driver_context_get_udp_channel_transport_bindings(
    aeron_driver_context_t *context)
{
    return NULL != context ?
        context->udp_channel_transport_bindings :
        aeron_udp_channel_transport_bindings_load_media(AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT);
}

int aeron_driver_context_set_udp_channel_outgoing_interceptors(
    aeron_driver_context_t *context, aeron_udp_channel_interceptor_bindings_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->udp_channel_outgoing_interceptor_bindings = value;
    return 0;
}

aeron_udp_channel_interceptor_bindings_t *aeron_driver_context_get_udp_channel_outgoing_interceptors(
    aeron_driver_context_t *context)
{
    return NULL != context ? context->udp_channel_outgoing_interceptor_bindings : NULL;
}

int aeron_driver_context_set_udp_channel_incoming_interceptors(
    aeron_driver_context_t *context, aeron_udp_channel_interceptor_bindings_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->udp_channel_incoming_interceptor_bindings = value;
    return 0;
}

aeron_udp_channel_interceptor_bindings_t *aeron_driver_context_get_udp_channel_incoming_interceptors(
    aeron_driver_context_t *context)
{
    return NULL != context ? context->udp_channel_incoming_interceptor_bindings : NULL;
}

int aeron_driver_context_set_receiver_group_consideration(
    aeron_driver_context_t *context, aeron_inferable_boolean_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->receiver_group_consideration = value;
    return 0;
}

aeron_inferable_boolean_t aeron_driver_context_get_receiver_group_consideration(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_group_consideration : AERON_RECEIVER_GROUP_CONSIDERATION_DEFAULT;
}

int aeron_driver_context_set_rejoin_stream(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->rejoin_stream = value;
    return 0;
}

int aeron_driver_context_set_publication_reserved_session_id_low(aeron_driver_context_t *context, int32_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_reserved_session_id_low = value;
    return 0;
}

int32_t aeron_driver_context_get_publication_reserved_session_id_low(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->publication_reserved_session_id_low : AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT;
}

int aeron_driver_context_set_publication_reserved_session_id_high(aeron_driver_context_t *context, int32_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_reserved_session_id_high = value;
    return 0;
}

int32_t aeron_driver_context_get_publication_reserved_session_id_high(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->publication_reserved_session_id_high : AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT;
}


bool aeron_driver_context_get_rejoin_stream(aeron_driver_context_t *context)
{
    return NULL != context ? context->rejoin_stream : AERON_REJOIN_STREAM_DEFAULT;
}
