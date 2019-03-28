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
#include "aeron_driver_context.h"
#include "aeron_alloc.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_transmitter.h"
#include "aeron_agent.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_termination_validator.h"

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

#define AERON_CONFIG_GETENV_OR_DEFAULT(e, d) ((NULL == getenv(e)) ? (d) : getenv(e))

static void aeron_driver_conductor_to_driver_interceptor_null(
    int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
}

static void aeron_driver_conductor_to_client_interceptor_null(
    aeron_driver_conductor_t *conductor, int32_t msg_type_id, const void *message, size_t length)
{
}

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
        "aeron_unicast_flow_control_strategy_supplier")) == NULL)
    {
        return -1;
    }

    if ((_context->multicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(
        "aeron_max_multicast_flow_control_strategy_supplier")) == NULL)
    {
        return -1;
    }

    if ((_context->congestion_control_supplier_func = aeron_congestion_control_strategy_supplier_load(
        "aeron_static_window_congestion_control_strategy_supplier")) == NULL)
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

    _context->threading_mode = AERON_THREADING_MODE_DEDICATED;
    _context->dirs_delete_on_start = false;
    _context->warn_if_dirs_exist = true;
    _context->term_buffer_sparse_file = false;
    _context->perform_storage_checks = true;
    _context->spies_simulate_connection = false;
    _context->driver_timeout_ms = 10 * 1000;
    _context->to_driver_buffer_length = 1024 * 1024 + AERON_RB_TRAILER_LENGTH;
    _context->to_clients_buffer_length = 1024 * 1024 + AERON_BROADCAST_BUFFER_TRAILER_LENGTH;
    _context->counters_values_buffer_length = 1024 * 1024;
    _context->counters_metadata_buffer_length =
        _context->counters_values_buffer_length *
        (AERON_COUNTERS_MANAGER_METADATA_LENGTH / AERON_COUNTERS_MANAGER_VALUE_LENGTH);
    _context->error_buffer_length = 1024 * 1024;
    _context->client_liveness_timeout_ns = 5 * 1000 * 1000 * 1000LL;
    _context->timer_interval_ns = 1 * 1000 * 1000 * 1000LL;
    _context->term_buffer_length = 16 * 1024 * 1024;
    _context->ipc_term_buffer_length = 64 * 1024 * 1024;
    _context->mtu_length = 1408;
    _context->ipc_mtu_length = 1408;
    _context->ipc_publication_window_length = 0;
    _context->publication_window_length = 0;
    _context->publication_linger_timeout_ns = 5 * 1000 * 1000 * 1000LL;
    _context->socket_rcvbuf = 128 * 1024;
    _context->socket_sndbuf = 0;
    _context->multicast_ttl = 0;
    _context->send_to_sm_poll_ratio = 4;
    _context->status_message_timeout_ns = 200 * 1000 * 1000LL;
    _context->image_liveness_timeout_ns = 10 * 1000 * 1000 * 1000LL;
    _context->initial_window_length = 128 * 1024;
    _context->loss_report_length = 1024 * 1024;
    _context->file_page_size = 4 * 1024;
    _context->publication_unblock_timeout_ns = 10 * 1000 * 1000 * 1000LL;
    _context->publication_connection_timeout_ns = 5 * 1000 * 1000 * 1000LL;
    _context->counter_free_to_reuse_ns = 1 * 1000 * 1000 * 1000LL;

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

    if ((value = getenv(AERON_THREADING_MODE_ENV_VAR)))
    {
        if (strncmp(value, "SHARED", sizeof("SHARED")) == 0)
        {
            _context->threading_mode = AERON_THREADING_MODE_SHARED;
        }
        else if (strncmp(value, "SHARED_NETWORK", sizeof("SHARED_NETWORK")) == 0)
        {
            _context->threading_mode = AERON_THREADING_MODE_SHARED_NETWORK;
        }
        else if (strncmp(value, "DEDICATED", sizeof("DEDICATED")) == 0)
        {
            _context->threading_mode = AERON_THREADING_MODE_DEDICATED;
        }
    }

    _context->dirs_delete_on_start = aeron_config_parse_bool(
        getenv(AERON_DIR_DELETE_ON_START_ENV_VAR),
        _context->dirs_delete_on_start);

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

    _context->counters_metadata_buffer_length =
        _context->counters_values_buffer_length *
        (AERON_COUNTERS_MANAGER_METADATA_LENGTH / AERON_COUNTERS_MANAGER_VALUE_LENGTH);

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

    _context->to_driver_buffer = NULL;
    _context->to_clients_buffer = NULL;
    _context->counters_values_buffer = NULL;
    _context->counters_metadata_buffer = NULL;
    _context->error_buffer = NULL;

    _context->nano_clock = aeron_nano_clock;
    _context->epoch_clock = aeron_epoch_clock;

    _context->conductor_idle_strategy_init_args =
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_CONDUCTOR_IDLE_STRATEGY_INIT_ARGS_ENV_VAR, NULL);
    if ((_context->conductor_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->conductor_idle_strategy_state,
        AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR,
        _context->conductor_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->shared_idle_strategy_init_args =
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SHARED_IDLE_STRATEGY_ENV_INIT_ARGS_VAR, NULL);
    if ((_context->shared_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SHARED_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->shared_idle_strategy_state,
        AERON_SHARED_IDLE_STRATEGY_ENV_VAR,
        _context->shared_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->shared_network_idle_strategy_init_args =
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SHAREDNETWORK_IDLE_STRATEGY_INIT_ARGS_ENV_VAR, NULL);
    if ((_context->shared_network_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->shared_network_idle_strategy_state,
        AERON_SHARED_IDLE_STRATEGY_ENV_VAR,
        _context->shared_network_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->sender_idle_strategy_init_args =
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SENDER_IDLE_STRATEGY_INIT_ARGS_ENV_VAR, NULL);
    if ((_context->sender_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SENDER_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->sender_idle_strategy_state,
        AERON_SENDER_IDLE_STRATEGY_ENV_VAR,
        _context->sender_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    _context->receiver_idle_strategy_init_args =
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_RECEIVER_IDLE_STRATEGY_INIT_ARGS_ENV_VAR, NULL);
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

#ifdef HAVE_UUID_GENERATE
    uuid_t id;
    uuid_generate(id);

    struct uuid_as_uint64
    {
        uint64_t high;
        uint64_t low;
    }
        *id_as_uint64 = (struct uuid_as_uint64 *)&id;
    _context->receiver_id = id_as_uint64->high ^ id_as_uint64->low;
#else
    /* pure random id */
    _context->receiver_id = aeron_randomised_int32();
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
    aeron_mapped_file_t *cnc_mmap, int64_t timeout, int64_t now, aeron_log_func_t log_func)
{
    char buffer[AERON_MAX_PATH];
    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)cnc_mmap->addr;
    int32_t cnc_version;

    while (0 == (cnc_version = aeron_cnc_version_volatile(metadata)))
    {
        if (aeron_epoch_clock() > (now + timeout))
        {
            snprintf(buffer, sizeof(buffer) - 1, "ERROR: aeron cnc file version was 0 for timeout");
            return false;
        }

        aeron_micro_sleep(1000);
    }

    if (AERON_CNC_VERSION != cnc_version)
    {
        snprintf(
            buffer, sizeof(buffer) - 1,
            "ERROR: aeron cnc version does not match: file version=%d software version=%d",
            cnc_version, AERON_CNC_VERSION);
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
            int64_t timestamp = aeron_mpsc_rb_consumer_heartbeat_time_value(&rb);
            int64_t diff = now - timestamp;

            snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron driver heartbeat is %" PRId64 " ms old", diff);
            log_func(buffer);

            if (diff <= timeout)
            {
                return true;
            }
        }
    }

    return false;
}

bool aeron_is_driver_active(const char *dirname, int64_t timeout, int64_t now, aeron_log_func_t log_func)
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

        result = aeron_is_driver_active_with_cnc(&cnc_map, timeout, aeron_epoch_clock(), log_func);

        aeron_unmap(&cnc_map);
    }

    return result;
}

extern int32_t aeron_cnc_version_volatile(aeron_cnc_metadata_t *metadata);

extern void aeron_cnc_version_signal_cnc_ready(aeron_cnc_metadata_t *metadata, int32_t cnc_version);

extern uint8_t *aeron_cnc_to_driver_buffer(aeron_cnc_metadata_t *metadata);

extern uint8_t *aeron_cnc_to_clients_buffer(aeron_cnc_metadata_t *metadata);

extern uint8_t *aeron_cnc_counters_metadata_buffer(aeron_cnc_metadata_t *metadata);

extern uint8_t *aeron_cnc_counters_values_buffer(aeron_cnc_metadata_t *metadata);

extern uint8_t *aeron_cnc_error_log_buffer(aeron_cnc_metadata_t *metadata);

extern size_t aeron_cnc_computed_length(size_t total_length_of_buffers, size_t alignment);

extern size_t aeron_cnc_length(aeron_driver_context_t *context);

extern size_t aeron_ipc_publication_term_window_length(aeron_driver_context_t *context, size_t term_length);

extern size_t aeron_network_publication_term_window_length(aeron_driver_context_t *context, size_t term_length);
