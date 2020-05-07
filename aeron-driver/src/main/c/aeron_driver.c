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
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include "util/aeron_platform.h"
#if defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
#define _CRT_RAND_S

#define S_IRWXU 0
#define S_IRWXG 0
#define S_IRWXO 0
#endif
#include <stdlib.h>
#include <stddef.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>
#include <fcntl.h>

#include "aeron_windows.h"
#include <inttypes.h>
#include "util/aeron_error.h"
#include "aeronmd.h"
#include "aeron_alloc.h"
#include "util/aeron_strutil.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver.h"
#include "aeron_socket.h"
#include "util/aeron_dlopen.h"
#include "aeron_driver_context.h"

const char aeron_version_full_str[] = "aeron version " AERON_VERSION_TXT " built " __DATE__ " " __TIME__;
int aeron_major_version = AERON_VERSION_MAJOR;
int aeron_minor_version = AERON_VERSION_MINOR;
int aeron_patch_version = AERON_VERSION_PATCH;

const char *aeron_version_full()
{
    return aeron_version_full_str;
}

int aeron_version_major()
{
    return aeron_major_version;
}

int aeron_version_minor()
{
    return aeron_minor_version;
}

int aeron_version_patch()
{
    return aeron_patch_version;
}

int32_t aeron_semantic_version_compose(uint8_t major, uint8_t minor, uint8_t patch)
{
    return (major << 16) | (minor << 8) | patch;
}

uint8_t aeron_semantic_version_major(int32_t version)
{
    return (uint8_t)((version >> 16) & 0xFF);
}

uint8_t aeron_semantic_version_minor(int32_t version)
{
    return (uint8_t)((version >> 8) & 0xFF);
}

uint8_t aeron_semantic_version_patch(int32_t version)
{
    return (uint8_t)(version & 0xFF);
}

void aeron_log_func_stderr(const char *str)
{
    fprintf(stderr, "%s\n", str);
}

void aeron_log_func_none(const char *str)
{
}

extern int aeron_number_of_trailing_zeroes(int32_t value);
extern int aeron_number_of_trailing_zeroes_u64(uint64_t value);
extern int32_t aeron_find_next_power_of_two(int32_t value);

static void error_log_reader_save_to_file(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    FILE *saved_errors_file = (FILE *)clientd;
    char first_datestamp[AERON_MAX_PATH];
    char last_datestamp[AERON_MAX_PATH];

    aeron_format_date(first_datestamp, sizeof(first_datestamp) - 1, first_observation_timestamp);
    aeron_format_date(last_datestamp, sizeof(last_datestamp) - 1, last_observation_timestamp);
    fprintf(
        saved_errors_file,
        "***\n%d observations from %s to %s for:\n %.*s\n",
        observation_count,
        first_datestamp,
        last_datestamp,
        (int)error_length,
        error);
}

int aeron_report_existing_errors(aeron_mapped_file_t *cnc_map, const char *aeron_dir)
{
    char buffer[AERON_MAX_PATH * 2];
    int result = 0;

    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)cnc_map->addr;

    if (aeron_semantic_version_major(AERON_CNC_VERSION) == aeron_semantic_version_major(metadata->cnc_version) &&
        aeron_error_log_exists(aeron_cnc_error_log_buffer(cnc_map->addr), (size_t)metadata->error_log_buffer_length))
    {
        char datestamp[AERON_MAX_PATH];
        FILE *saved_errors_file = NULL;

        aeron_format_date(datestamp, sizeof(datestamp) - 1, aeron_epoch_clock());
        while (true)
        {
            char* invalid_win_symbol = strstr(datestamp, ":");
            if (invalid_win_symbol == NULL)
            {
                break;
            }
            *invalid_win_symbol = '-';
        }
        snprintf(buffer, sizeof(buffer) - 1, "%s-%s-error.log", aeron_dir, datestamp);

        if ((saved_errors_file = fopen(buffer, "w")) != NULL)
        {
            uint64_t observations = aeron_error_log_read(
                aeron_cnc_error_log_buffer(metadata),
                (size_t)metadata->error_log_buffer_length,
                error_log_reader_save_to_file,
                saved_errors_file,
                0);

            fprintf(saved_errors_file, "\n%" PRIu64 " distinct errors observed.\n", observations);
            fprintf(stderr, "WARNING: Existing errors saved to: %s\n", buffer);

            fclose(saved_errors_file);
        }
        else
        {
            aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
            result = -1;
        }
    }

    return result;
}

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context)
{
    char buffer[AERON_MAX_PATH];
    const char *dirname = context->aeron_dir;
    aeron_log_func_t log_func = aeron_log_func_none;

    if (aeron_is_directory(dirname))
    {
        if (context->warn_if_dirs_exist)
        {
            log_func = aeron_log_func_stderr;
            snprintf(buffer, sizeof(buffer) - 1, "WARNING: %s exists", dirname);
            log_func(buffer);
        }

        if (context->dirs_delete_on_start)
        {
            aeron_delete_directory(context->aeron_dir);
        }
        else
        {
            aeron_mapped_file_t cnc_mmap = { NULL, 0 };

            snprintf(buffer, sizeof(buffer) - 1, "%s/%s", dirname, AERON_CNC_FILE);
            if (aeron_map_existing_file(&cnc_mmap, buffer) < 0)
            {
                snprintf(buffer, sizeof(buffer) - 1, "INFO: failed to mmap CnC file");
                log_func(buffer);
                return -1;
            }

            snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron CnC file %s/%s exists", dirname, AERON_CNC_FILE);
            log_func(buffer);

            if (aeron_is_driver_active_with_cnc(
                &cnc_mmap, context->driver_timeout_ms, aeron_epoch_clock(), log_func))
            {
                aeron_unmap(&cnc_mmap);
                return -1;
            }

            if (aeron_report_existing_errors(&cnc_mmap, dirname) < 0)
            {
                aeron_unmap(&cnc_mmap);
                return -1;
            }

            aeron_unmap(&cnc_mmap);
            aeron_delete_directory(context->aeron_dir);
        }
    }

    if (aeron_mkdir(context->aeron_dir, S_IRWXU | S_IRWXG | S_IRWXO) != 0)
    {
        aeron_set_err_from_last_err_code("mkdir %s", context->aeron_dir);
        return -1;
    }

    snprintf(buffer, sizeof(buffer) - 1, "%s/%s", dirname, AERON_PUBLICATIONS_DIR);
    if (aeron_mkdir(buffer, S_IRWXU | S_IRWXG | S_IRWXO) != 0)
    {
        aeron_set_err_from_last_err_code("mkdir %s", buffer);
        return -1;
    }

    snprintf(buffer, sizeof(buffer) - 1, "%s/%s", dirname, AERON_IMAGES_DIR);
    if (aeron_mkdir(buffer, S_IRWXU | S_IRWXG | S_IRWXO) != 0)
    {
        aeron_set_err_from_last_err_code("mkdir %s", buffer);
        return -1;
    }

    return 0;
}

void aeron_driver_fill_cnc_metadata(aeron_driver_context_t *context)
{
    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)context->cnc_map.addr;
    metadata->to_driver_buffer_length = (int32_t)context->to_driver_buffer_length;
    metadata->to_clients_buffer_length = (int32_t)context->to_clients_buffer_length;
    metadata->counter_metadata_buffer_length =
        (int32_t)(AERON_COUNTERS_METADATA_BUFFER_LENGTH(context->counters_values_buffer_length));
    metadata->counter_values_buffer_length = (int32_t)context->counters_values_buffer_length;
    metadata->error_log_buffer_length = (int32_t)context->error_buffer_length;
    metadata->client_liveness_timeout = (int64_t)context->client_liveness_timeout_ns;
    metadata->start_timestamp = context->epoch_clock();
    metadata->pid = getpid();

    context->to_driver_buffer = aeron_cnc_to_driver_buffer(metadata);
    context->to_clients_buffer = aeron_cnc_to_clients_buffer(metadata);
    context->counters_values_buffer = aeron_cnc_counters_values_buffer(metadata);
    context->counters_metadata_buffer = aeron_cnc_counters_metadata_buffer(metadata);
    context->error_buffer = aeron_cnc_error_log_buffer(metadata);
}

int aeron_driver_create_cnc_file(aeron_driver_t *driver)
{
    char buffer[AERON_MAX_PATH];
    size_t cnc_file_length = aeron_cnc_length(driver->context);

    driver->context->cnc_map.addr = NULL;
    driver->context->cnc_map.length = cnc_file_length;

    snprintf(buffer, sizeof(buffer) - 1, "%s/%s", driver->context->aeron_dir, AERON_CNC_FILE);

    if (aeron_map_new_file(&driver->context->cnc_map, buffer, true) < 0)
    {
        aeron_set_err(aeron_errcode(), "could not map CnC file: %s", aeron_errmsg());
        return -1;
    }

    aeron_driver_fill_cnc_metadata(driver->context);

    return 0;
}

int aeron_driver_create_loss_report_file(aeron_driver_t *driver)
{
    char buffer[AERON_MAX_PATH];

    driver->context->loss_report.addr = NULL;
    driver->context->loss_report.length =
        AERON_ALIGN(driver->context->loss_report_length, driver->context->file_page_size);

    snprintf(buffer, sizeof(buffer) - 1, "%s/%s", driver->context->aeron_dir, AERON_LOSS_REPORT_FILE);

    if (aeron_map_new_file(&driver->context->loss_report, buffer, true) < 0)
    {
        aeron_set_err(aeron_errcode(), "could not map loss report file: %s", aeron_errmsg());
        return -1;
    }

    return 0;
}

int aeron_driver_validate_sufficient_socket_buffer_lengths(aeron_driver_t *driver)
{
    int result = -1;
    aeron_socket_t probe_fd;

    if ((probe_fd = aeron_socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    {
        aeron_set_err_from_last_err_code("socket %s:%d", __FILE__, __LINE__);
        goto cleanup;
    }

    size_t default_sndbuf = 0;
    socklen_t len = sizeof(default_sndbuf);
    if (aeron_getsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &default_sndbuf, &len) < 0)
    {
        aeron_set_err_from_last_err_code("getsockopt(SO_SNDBUF) %s:%d", __FILE__, __LINE__);
        goto cleanup;
    }

    size_t default_rcvbuf = 0;
    len = sizeof(default_rcvbuf);
    if (aeron_getsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &default_sndbuf, &len) < 0)
    {
        aeron_set_err_from_last_err_code("getsockopt(SO_RCVBUF) %s:%d", __FILE__, __LINE__);
        goto cleanup;
    }

    size_t max_rcvbuf = default_rcvbuf;
    size_t max_sndbuf = default_sndbuf;

    if (driver->context->socket_sndbuf > 0)
    {
        size_t socket_sndbuf = driver->context->socket_sndbuf;

        if (aeron_setsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &socket_sndbuf, sizeof(socket_sndbuf)) < 0)
        {
            aeron_set_err_from_last_err_code("setsockopt(SO_SNDBUF) %s:%d", __FILE__, __LINE__);
            goto cleanup;
        }

        len = sizeof(socket_sndbuf);
        if (aeron_getsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &socket_sndbuf, &len) < 0)
        {
            aeron_set_err_from_last_err_code("getsockopt(SO_SNDBUF) %s:%d", __FILE__, __LINE__);
            goto cleanup;
        }

        max_sndbuf = socket_sndbuf;

        if (driver->context->socket_sndbuf > socket_sndbuf)
        {
            fprintf(
                stderr,
                "WARNING: Could not get desired SO_SNDBUF, adjust OS buffer to match %s: attempted=%" PRIu64 ", actual=%" PRIu64 "\n",
                AERON_SOCKET_SO_SNDBUF_ENV_VAR,
                (uint64_t)driver->context->socket_sndbuf,
                (uint64_t)socket_sndbuf);
        }
    }

    if (driver->context->socket_rcvbuf > 0)
    {
        size_t socket_rcvbuf = driver->context->socket_rcvbuf;

        if (aeron_setsockopt(probe_fd, SOL_SOCKET, SO_RCVBUF, &socket_rcvbuf, sizeof(socket_rcvbuf)) < 0)
        {
            aeron_set_err_from_last_err_code("setsockopt(SO_RCVBUF) %s:%d", __FILE__, __LINE__);
            goto cleanup;
        }

        len = sizeof(socket_rcvbuf);
        if (aeron_getsockopt(probe_fd, SOL_SOCKET, SO_RCVBUF, &socket_rcvbuf, &len) < 0)
        {
            aeron_set_err_from_last_err_code("getsockopt(SO_RCVBUF) %s:%d", __FILE__, __LINE__);
            goto cleanup;
        }

        max_rcvbuf = socket_rcvbuf;

        if (driver->context->socket_rcvbuf > socket_rcvbuf)
        {
            fprintf(
                stderr,
                "WARNING: Could not get desired SO_RCVBUF, adjust OS buffer to match %s: attempted=%" PRIu64 ", actual=%" PRIu64 "\n",
                AERON_SOCKET_SO_RCVBUF_ENV_VAR,
                (uint64_t)driver->context->socket_rcvbuf,
                (uint64_t)socket_rcvbuf);
        }
    }

    if (driver->context->mtu_length > max_sndbuf)
    {
        aeron_set_err(
            EINVAL,
            "MTU greater than socket SO_SNDBUF, adjust %s to match MTU: mtuLength=%" PRIu64 ", SO_SNDBUF=%" PRIu64 "\n",
            AERON_SOCKET_SO_SNDBUF_ENV_VAR,
            (uint64_t)driver->context->mtu_length,
            max_sndbuf);
        goto cleanup;
    }

    if (driver->context->initial_window_length > max_rcvbuf)
    {
        aeron_set_err(
            EINVAL,
            "Window length greater than socket SO_RCVBUF, increase %s to match window: windowLength=%" PRIu64 ", SO_RCVBUF=%" PRIu64 "\n",
            AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR,
            (uint64_t)driver->context->initial_window_length,
            max_rcvbuf);
        goto cleanup;
    }

    result = 0;

    cleanup:
        aeron_close_socket(probe_fd);

    return result;
}

int aeron_driver_validate_page_size(aeron_driver_t *driver)
{
    if (driver->context->file_page_size < AERON_PAGE_MIN_SIZE)
    {
        aeron_set_err(
            EINVAL,
            "Page size less than min size of %" PRIu64 ": page size=%" PRIu64,
            AERON_PAGE_MIN_SIZE, driver->context->file_page_size);
        return -1;
    }

    if (driver->context->file_page_size > AERON_PAGE_MAX_SIZE)
    {
        aeron_set_err(
            EINVAL,
            "Page size greater than max size of %" PRIu64 ": page size=%" PRIu64,
            AERON_PAGE_MAX_SIZE, driver->context->file_page_size);
        return -1;
    }

    if (!AERON_IS_POWER_OF_TWO(driver->context->file_page_size))
    {
        aeron_set_err(
            EINVAL,
            "Page size not a power of 2: page size=%" PRIu64,
            driver->context->file_page_size);
        return -1;
    }

    return 0;
}

void aeron_driver_context_print_configuration(aeron_driver_context_t *context)
{
    FILE *fpout = stdout;
    char buffer[1024];

    fprintf(fpout, "aeron_driver_context_t {");
    fprintf(fpout, "\n    cnc_version=%d.%d.%d",
        (int)aeron_semantic_version_major(AERON_CNC_VERSION),
        (int)aeron_semantic_version_minor(AERON_CNC_VERSION),
        (int)aeron_semantic_version_patch(AERON_CNC_VERSION));
    fprintf(fpout, "\n    aeron_dir=%s", context->aeron_dir);
    fprintf(fpout, "\n    driver_timeout_ms=%" PRIu64, context->driver_timeout_ms);
    fprintf(fpout, "\n    print_configuration_on_start=%d", context->print_configuration_on_start);
    fprintf(fpout, "\n    dirs_delete_on_start=%d", context->dirs_delete_on_start);
    fprintf(fpout, "\n    dirs_delete_on_shutdown=%d", context->dirs_delete_on_shutdown);
    fprintf(fpout, "\n    warn_if_dirs_exists=%d", context->warn_if_dirs_exist);
    fprintf(fpout, "\n    term_buffer_sparse_file=%d", context->term_buffer_sparse_file);
    fprintf(fpout, "\n    perform_storage_checks=%d", context->perform_storage_checks);
    fprintf(fpout, "\n    spies_simulate_connection=%d", context->spies_simulate_connection);
    fprintf(fpout, "\n    reliable_stream=%d", context->reliable_stream);
    fprintf(fpout, "\n    tether_subscriptions=%d", context->tether_subscriptions);
    fprintf(fpout, "\n    rejoin_stream=%d", context->rejoin_stream);
    fprintf(fpout, "\n    receiver_group_consideration=%d", context->receiver_group_consideration);
    fprintf(fpout, "\n    to_driver_buffer_length=%" PRIu64, (uint64_t)context->to_driver_buffer_length);
    fprintf(fpout, "\n    to_clients_buffer_length=%" PRIu64, (uint64_t)context->to_clients_buffer_length);
    fprintf(fpout, "\n    counters_values_buffer_length=%" PRIu64, (uint64_t)context->counters_values_buffer_length);
    fprintf(fpout, "\n    error_buffer_length=%" PRIu64, (uint64_t)context->error_buffer_length);
    fprintf(fpout, "\n    timer_interval_ns=%" PRIu64, context->timer_interval_ns);
    fprintf(fpout, "\n    client_liveness_timeout_ns=%" PRIu64, context->client_liveness_timeout_ns);
    fprintf(fpout, "\n    image_liveness_timeout_ns=%" PRIu64, context->image_liveness_timeout_ns);
    fprintf(fpout, "\n    publication_unblock_timeout_ns=%" PRIu64, context->publication_unblock_timeout_ns);
    fprintf(fpout, "\n    publication_connection_timeout_ns=%" PRIu64, context->publication_connection_timeout_ns);
    fprintf(fpout, "\n    publication_linger_timeout_ns=%" PRIu64, context->publication_linger_timeout_ns);
    fprintf(fpout, "\n    untethered_window_limit_timeout_ns=%" PRIu64, context->untethered_window_limit_timeout_ns);
    fprintf(fpout, "\n    untethered_resting_timeout_ns=%" PRIu64, context->untethered_resting_timeout_ns);
    fprintf(fpout, "\n    retransmit_unicast_delay_ns=%" PRIu64, context->retransmit_unicast_delay_ns);
    fprintf(fpout, "\n    retransmit_unicast_linger_ns=%" PRIu64, context->retransmit_unicast_linger_ns);
    fprintf(fpout, "\n    nak_unicast_delay_ns=%" PRIu64, context->nak_unicast_delay_ns);
    fprintf(fpout, "\n    nak_multicast_max_backoff_ns=%" PRIu64, context->nak_multicast_max_backoff_ns);
    fprintf(fpout, "\n    nak_multicast_group_size=%" PRIu64, (uint64_t)context->nak_multicast_group_size);
    fprintf(fpout, "\n    status_message_timeout_ns=%" PRIu64, context->status_message_timeout_ns);
    fprintf(fpout, "\n    counter_free_to_reuse_ns=%" PRIu64, context->counter_free_to_reuse_ns);
    fprintf(fpout, "\n    term_buffer_length=%" PRIu64, (uint64_t)context->term_buffer_length);
    fprintf(fpout, "\n    ipc_term_buffer_length=%" PRIu64, (uint64_t)context->ipc_term_buffer_length);
    fprintf(fpout, "\n    publication_window_length=%" PRIu64, (uint64_t)context->publication_window_length);
    fprintf(fpout, "\n    ipc_publication_window_length=%" PRIu64, (uint64_t)context->ipc_publication_window_length);
    fprintf(fpout, "\n    initial_window_length=%" PRIu64, (uint64_t)context->initial_window_length);
    fprintf(fpout, "\n    socket_sndbuf=%" PRIu64, (uint64_t)context->socket_sndbuf);
    fprintf(fpout, "\n    socket_rcvbuf=%" PRIu64, (uint64_t)context->socket_rcvbuf);
    fprintf(fpout, "\n    multicast_ttl=%" PRIu8, context->multicast_ttl);
    fprintf(fpout, "\n    mtu_length=%" PRIu64, (uint64_t)context->mtu_length);
    fprintf(fpout, "\n    ipc_mtu_length=%" PRIu64, (uint64_t)context->ipc_mtu_length);
    fprintf(fpout, "\n    file_page_size=%" PRIu64, (uint64_t)context->file_page_size);
    fprintf(fpout, "\n    publication_reserved_session_id_low=%" PRId32, context->publication_reserved_session_id_low);
    fprintf(fpout, "\n    publication_reserved_session_id_high=%" PRId32, context->publication_reserved_session_id_high);
    fprintf(fpout, "\n    loss_report_length=%" PRIu64, (uint64_t)context->loss_report_length);
    fprintf(fpout, "\n    send_to_sm_poll_ratio=%" PRIu64, (uint64_t)context->send_to_sm_poll_ratio);

#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif

    fprintf(fpout, "\n    epoch_clock=%p%s",
        (void *)context->epoch_clock,
        aeron_dlinfo((const void *)context->epoch_clock, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    nano_clock=%p%s",
        (void *)context->nano_clock,
        aeron_dlinfo((const void *)context->nano_clock, buffer, sizeof(buffer)));
    /* cachedEpochClock */
    /* cachedNanoClock */
    fprintf(fpout, "\n    threading_mode=%d", context->threading_mode);
    fprintf(fpout, "\n    agent_on_start_func=%p%s",
        (void *)context->agent_on_start_func,
        aeron_dlinfo((const void *)context->agent_on_start_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    agent_on_start_state=%p", context->agent_on_start_state);
    fprintf(fpout, "\n    conductor_idle_strategy_func=%p%s",
        (void *)context->conductor_idle_strategy_func,
        aeron_dlinfo((const void *)context->conductor_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    conductor_idle_strategy_init_args=%p%s",
        (void *)context->conductor_idle_strategy_init_args,
        context->conductor_idle_strategy_init_args ? context->conductor_idle_strategy_init_args : "");
    fprintf(fpout, "\n    sender_idle_strategy_func=%p%s",
        (void *)context->sender_idle_strategy_func,
        aeron_dlinfo((const void *)context->sender_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    sender_idle_strategy_init_args=%p%s",
        (void *)context->sender_idle_strategy_init_args,
        context->sender_idle_strategy_init_args ? context->sender_idle_strategy_init_args : "");
    fprintf(fpout, "\n    receiver_idle_strategy_func=%p%s",
        (void *)context->receiver_idle_strategy_func,
        aeron_dlinfo((const void *)context->receiver_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    receiver_idle_strategy_init_args=%p%s",
        (void *)context->receiver_idle_strategy_init_args,
        context->receiver_idle_strategy_init_args ? context->receiver_idle_strategy_init_args : "");
    fprintf(fpout, "\n    shared_network_idle_strategy_func=%p%s",
        (void *)context->shared_network_idle_strategy_func,
        aeron_dlinfo((const void *)context->shared_network_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    shared_network_idle_strategy_init_args=%p%s",
        (void *)context->shared_network_idle_strategy_init_args,
        context->shared_network_idle_strategy_init_args ? context->shared_network_idle_strategy_init_args : "");
    fprintf(fpout, "\n    shared_idle_strategy_func=%p%s",
        (void *)context->shared_idle_strategy_func,
        aeron_dlinfo((const void *)context->shared_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    shared_idle_strategy_init_args=%p%s",
        (void *)context->shared_idle_strategy_init_args,
        context->shared_idle_strategy_init_args ? context->shared_idle_strategy_init_args : "");
    fprintf(fpout, "\n    unicast_flow_control_supplier_func=%p%s",
        (void *)context->unicast_flow_control_supplier_func,
        aeron_dlinfo((const void *)context->unicast_flow_control_supplier_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    multicast_flow_control_supplier_func=%p%s",
        (void *)context->multicast_flow_control_supplier_func,
        aeron_dlinfo((const void *)context->multicast_flow_control_supplier_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    receiver_group_tag.is_present=%d",
        context->receiver_group_tag.is_present);
    fprintf(fpout, "\n    receiver_group_tag.value=%" PRId64, context->receiver_group_tag.value);
    fprintf(fpout, "\n    flow_control.group_tag=%" PRId64, context->flow_control.group_tag);
    fprintf(fpout, "\n    flow_control.group_min_size=%" PRId32, context->flow_control.group_min_size);
    fprintf(fpout, "\n    flow_control_receiver_timeout_ns=%" PRIu64, context->flow_control.receiver_timeout_ns);
    fprintf(fpout, "\n    congestion_control_supplier_func=%p%s",
        (void *)context->congestion_control_supplier_func,
        aeron_dlinfo((const void *)context->congestion_control_supplier_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    usable_fs_space_func=%p%s",
        (void *)context->usable_fs_space_func,
        aeron_dlinfo((const void *)context->usable_fs_space_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    termination_validator_func=%p%s",
        (void *)context->termination_validator_func,
        aeron_dlinfo((const void *)context->termination_validator_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    termination_validator_state=%p", context->termination_validator_state);
    fprintf(fpout, "\n    termination_hook_func=%p%s",
        (void *)context->termination_hook_func,
        aeron_dlinfo((const void *)context->termination_hook_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    termination_hook_state=%p", context->termination_hook_state);
    fprintf(fpout, "\n    name_resolver_supplier_func=%p%s",
        (void *)context->name_resolver_supplier_func,
        aeron_dlinfo((const void *)context->name_resolver_supplier_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    name_resolver_init_args=%s",
        (void *)context->name_resolver_init_args ? context->name_resolver_init_args : "");
    fprintf(fpout, "\n    resolver_name=%s",
        (void *)context->resolver_name ? context->resolver_name : "");
    fprintf(fpout, "\n    resolver_interface=%s",
        (void *)context->resolver_interface ? context->resolver_interface : "");
    fprintf(fpout, "\n    resolver_bootstrap_neighbor=%s",
        (void *)context->resolver_bootstrap_neighbor ? context->resolver_bootstrap_neighbor : "");
    fprintf(fpout, "\n    re_resolution_check_interval_ns=%" PRIu64, context->re_resolution_check_interval_ns);

    const aeron_udp_channel_transport_bindings_t *bindings = context->udp_channel_transport_bindings;
    while (NULL != bindings)
    {
        fprintf(
            fpout, "\n    udp_channel_transport_bindings.%s=%s,%p%s",
            bindings->meta_info.type, bindings->meta_info.name,
            bindings->meta_info.source_symbol, aeron_dlinfo(bindings->meta_info.source_symbol, buffer, sizeof(buffer)));

        bindings = bindings->meta_info.next_binding;
    }

    const aeron_udp_channel_interceptor_bindings_t *interceptor_bindings;

    interceptor_bindings = context->udp_channel_outgoing_interceptor_bindings;
    while (NULL != interceptor_bindings)
    {
        fprintf(
            fpout, "\n    udp_channel_outgoing_interceptor_bindings.%s=%s,%p%s",
            interceptor_bindings->meta_info.type,
            interceptor_bindings->meta_info.name,
            interceptor_bindings->meta_info.source_symbol,
            aeron_dlinfo(interceptor_bindings->meta_info.source_symbol, buffer, sizeof(buffer)));

        interceptor_bindings = interceptor_bindings->meta_info.next_interceptor_bindings;
    }

    interceptor_bindings = context->udp_channel_incoming_interceptor_bindings;
    while (NULL != interceptor_bindings)
    {
        fprintf(
            fpout, "\n    udp_channel_incoming_interceptor_bindings.%s=%s,%p%s",
            interceptor_bindings->meta_info.type,
            interceptor_bindings->meta_info.name,
            interceptor_bindings->meta_info.source_symbol,
            aeron_dlinfo(interceptor_bindings->meta_info.source_symbol, buffer, sizeof(buffer)));

        interceptor_bindings = interceptor_bindings->meta_info.next_interceptor_bindings;
    }

#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif

    fprintf(fpout, "\n}\n");
}

int aeron_driver_shared_do_work(void *clientd)
{
    aeron_driver_t *driver = (aeron_driver_t *)clientd;
    int sum = 0;

    sum += aeron_driver_conductor_do_work(&driver->conductor);
    sum += aeron_driver_sender_do_work(&driver->sender);
    sum += aeron_driver_receiver_do_work(&driver->receiver);

    return sum;
}

void aeron_driver_shared_on_close(void *clientd)
{
    aeron_driver_t *driver = (aeron_driver_t *)clientd;

    aeron_driver_conductor_on_close(&driver->conductor);
    aeron_driver_sender_on_close(&driver->sender);
    aeron_driver_receiver_on_close(&driver->receiver);
}

int aeron_driver_shared_network_do_work(void *clientd)
{
    aeron_driver_t *driver = (aeron_driver_t *)clientd;
    int sum = 0;

    sum += aeron_driver_sender_do_work(&driver->sender);
    sum += aeron_driver_receiver_do_work(&driver->receiver);

    return sum;
}

void aeron_driver_shared_network_on_close(void *clientd)
{
    aeron_driver_t *driver = (aeron_driver_t *)clientd;

    aeron_driver_sender_on_close(&driver->sender);
    aeron_driver_receiver_on_close(&driver->receiver);
}

int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context)
{
    aeron_driver_t *_driver = NULL;

    if (NULL == driver || NULL == context)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_driver_init: %s", strerror(EINVAL));
        goto error;
    }

    if (aeron_alloc((void **)&_driver, sizeof(aeron_driver_t)) < 0)
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        goto error;
    }

    _driver->context = context;

    for (int i = 0; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        _driver->runners[i].state = AERON_AGENT_STATE_UNUSED;
        _driver->runners[i].role_name = NULL;
        _driver->runners[i].on_close = NULL;
    }

    if (aeron_logbuffer_check_term_length(_driver->context->term_buffer_length) < 0 ||
        aeron_logbuffer_check_term_length(_driver->context->ipc_term_buffer_length) < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_page_size(_driver) < 0)
    {
        goto error;
    }

    if (aeron_driver_context_validate_mtu_length(_driver->context->mtu_length) < 0 ||
        aeron_driver_context_validate_mtu_length(_driver->context->ipc_mtu_length) < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_sufficient_socket_buffer_lengths(_driver) < 0)
    {
        goto error;
    }

    if (aeron_driver_ensure_dir_is_recreated(_driver->context) < 0)
    {
        aeron_set_err(
            aeron_errcode(), "could not recreate aeron dir %s: %s", _driver->context->aeron_dir, aeron_errmsg());
        goto error;
    }

    if (aeron_driver_create_cnc_file(_driver) < 0)
    {
        goto error;
    }

    if (aeron_driver_create_loss_report_file(_driver) < 0)
    {
        goto error;
    }

    context->counters_manager = &_driver->conductor.counters_manager;
    context->system_counters = &_driver->conductor.system_counters;
    context->error_log = &_driver->conductor.error_log;
    _driver->context->conductor_proxy = &_driver->conductor.conductor_proxy;

    if (aeron_driver_conductor_init(&_driver->conductor, context) < 0)
    {
        goto error;
    }

    if (aeron_driver_sender_init(
        &_driver->sender, context, &_driver->conductor.system_counters, &_driver->conductor.error_log) < 0)
    {
        goto error;
    }

    _driver->context->sender_proxy = &_driver->sender.sender_proxy;

    if (aeron_driver_receiver_init(
        &_driver->receiver, context, &_driver->conductor.system_counters, &_driver->conductor.error_log) < 0)
    {
        goto error;
    }

    _driver->context->receiver_proxy = &_driver->receiver.receiver_proxy;

    aeron_mpsc_rb_consumer_heartbeat_time(&_driver->conductor.to_driver_commands, aeron_epoch_clock());
    aeron_cnc_version_signal_cnc_ready((aeron_cnc_metadata_t *)context->cnc_map.addr, AERON_CNC_VERSION);

    if (aeron_feedback_delay_state_init(
        &_driver->context->unicast_delay_feedback_generator,
        aeron_loss_detector_nak_multicast_delay_generator,
        _driver->context->nak_unicast_delay_ns,
        1,
        true) < 0)
    {
        goto error;
    }

    if (aeron_feedback_delay_state_init(
        &_driver->context->multicast_delay_feedback_generator,
        aeron_loss_detector_nak_unicast_delay_generator,
        _driver->context->nak_multicast_max_backoff_ns,
        _driver->context->nak_multicast_group_size,
        false) < 0)
    {
        goto error;
    }

    if (_driver->context->print_configuration_on_start)
    {
        aeron_driver_context_print_configuration(_driver->context);
    }

    switch (_driver->context->threading_mode)
    {
        case AERON_THREADING_MODE_INVOKER:
        case AERON_THREADING_MODE_SHARED:
            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_SHARED],
                "[conductor, sender, receiver]",
                _driver,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_shared_do_work,
                aeron_driver_shared_on_close,
                _driver->context->shared_idle_strategy_func,
                _driver->context->shared_idle_strategy_state) < 0)
            {
                goto error;
            }
            break;

        case AERON_THREADING_MODE_SHARED_NETWORK:
            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_CONDUCTOR],
                "conductor",
                &_driver->conductor,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_conductor_do_work,
                aeron_driver_conductor_on_close,
                _driver->context->conductor_idle_strategy_func,
                _driver->context->conductor_idle_strategy_state) < 0)
            {
                goto error;
            }

            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_SHARED_NETWORK],
                "[sender, receiver]",
                _driver,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_shared_network_do_work,
                aeron_driver_shared_network_on_close,
                _driver->context->shared_network_idle_strategy_func,
                _driver->context->shared_network_idle_strategy_state) < 0)
            {
                goto error;
            }
            break;

        case AERON_THREADING_MODE_DEDICATED:
        default:
            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_CONDUCTOR],
                "conductor",
                &_driver->conductor,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_conductor_do_work,
                aeron_driver_conductor_on_close,
                _driver->context->conductor_idle_strategy_func,
                _driver->context->conductor_idle_strategy_state) < 0)
            {
                goto error;
            }

            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_SENDER],
                "sender",
                &_driver->sender,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_sender_do_work,
                aeron_driver_sender_on_close,
                _driver->context->sender_idle_strategy_func,
                _driver->context->sender_idle_strategy_state) < 0)
            {
                goto error;
            }

            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_RECEIVER],
                "receiver",
                &_driver->receiver,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_receiver_do_work,
                aeron_driver_receiver_on_close,
                _driver->context->receiver_idle_strategy_func,
                _driver->context->receiver_idle_strategy_state) < 0)
            {
                goto error;
            }
            break;
    }

    *driver = _driver;
    return 0;

    error:

    if (NULL != _driver)
    {
        aeron_free(_driver);
    }

    return -1;
}

int aeron_driver_start(aeron_driver_t *driver, bool manual_main_loop)
{
    if (NULL == driver)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_driver_start: %s", strerror(EINVAL));
        return -1;
    }

    if (!manual_main_loop)
    {
        if (AERON_THREADING_MODE_INVOKER == driver->context->threading_mode)
        {
            errno = EINVAL;
            aeron_set_err(EINVAL, "aeron_driver_start: %s", "INVOKER threading mode requires manual_main_loop");
            return -1;
        }

        if (aeron_agent_start(&driver->runners[0]) < 0)
        {
            return -1;
        }
    }
    else
    {
        if (NULL != driver->runners[0].on_start)
        {
            driver->runners[0].on_start(driver->runners[0].on_start_state, driver->runners[0].role_name);
        }

        driver->runners[0].state = AERON_AGENT_STATE_MANUAL;
    }

    for (int i = 1; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        if (driver->runners[i].state == AERON_AGENT_STATE_INITED)
        {
            if (aeron_agent_start(&driver->runners[i]) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int aeron_driver_main_do_work(aeron_driver_t *driver)
{
    if (NULL == driver)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_driver_main_do_work: %s", strerror(EINVAL));
        return -1;
    }

    return aeron_agent_do_work(&driver->runners[AERON_AGENT_RUNNER_CONDUCTOR]);
}

void aeron_driver_main_idle_strategy(aeron_driver_t *driver, int work_count)
{
    if (NULL == driver)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_driver_main_idle_strategy: %s", strerror(EINVAL));
        return;
    }

    aeron_agent_idle(&driver->runners[AERON_AGENT_RUNNER_CONDUCTOR], work_count);
}

int aeron_driver_close(aeron_driver_t *driver)
{
    if (NULL == driver)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_driver_close: %s", strerror(EINVAL));
        return -1;
    }

    for (int i = 0; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        if (aeron_agent_stop(&driver->runners[i]) < 0)
        {
            return -1;
        }
    }

    for (int i = 0; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        if (aeron_agent_close(&driver->runners[i]) < 0)
        {
            return -1;
        }
    }

    if (driver->context->dirs_delete_on_shutdown)
    {
        aeron_delete_directory(driver->context->aeron_dir);
    }

    aeron_free(driver);

    return 0;
}

extern int32_t aeron_semantic_version_compose(uint8_t major, uint8_t minor, uint8_t patch);

extern uint8_t aeron_semantic_version_major(int32_t version);

extern uint8_t aeron_semantic_version_minor(int32_t version);

extern uint8_t aeron_semantic_version_patch(int32_t version);
