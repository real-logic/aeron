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
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include <stddef.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdlib.h>
#include "util/aeron_error.h"
#include "aeronmd.h"
#include "aeron_alloc.h"
#include "util/aeron_strutil.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver.h"

void aeron_log_func_stderr(const char *str)
{
    fprintf(stderr, "%s\n", str);
}

void aeron_log_func_none(const char *str)
{
}

int64_t aeron_nano_clock()
{
    struct timespec ts;
#if defined(__CYGWIN__)
    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
    {
        return -1;
    }
#else
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) < 0)
    {
        return -1;
    }
#endif

    return (ts.tv_sec * 1000000000) + ts.tv_nsec;
}

int64_t aeron_epoch_clock()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) < 0)
    {
        return -1;
    }

    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

extern int aeron_number_of_trailing_zeroes(int32_t value);
extern int aeron_number_of_leading_zeroes(int32_t value);
extern int32_t aeron_find_next_power_of_two(int32_t value);

#ifndef HAVE_ARC4RANDOM
static int aeron_dev_random_fd = -1;
#endif

int32_t aeron_randomised_int32()
{
    int32_t result;

#ifdef HAVE_ARC4RANDOM
    uint32_t value = arc4random();

    memcpy(&result, &value, sizeof(int32_t));
#elif defined(__linux__) || defined(__CYGWIN__)
    if (-1 == aeron_dev_random_fd)
    {
        if ((aeron_dev_random_fd = open("/dev/urandom", O_RDONLY)) < 0)
        {
            fprintf(stderr, "could not open /dev/urandom (%d): %s\n", errno, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    if (sizeof(result) != read(aeron_dev_random_fd, &result, sizeof(result)))
    {
        fprintf(stderr, "Failed to read from aeron_dev_random (%d): %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }

#endif
    return result;
}

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

    if (AERON_CNC_VERSION == metadata->cnc_version &&
        aeron_error_log_exists(aeron_cnc_error_log_buffer(cnc_map->addr), (size_t)metadata->error_log_buffer_length))
    {
        char datestamp[AERON_MAX_PATH];
        FILE *saved_errors_file = NULL;

        aeron_format_date(datestamp, sizeof(datestamp) - 1, aeron_epoch_clock());
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
            int errcode = errno;

            aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
            result = -1;
        }
    }

    return result;
}

int aeron_driver_ensure_dir_is_recreated(aeron_driver_t *driver)
{
    struct stat sb;
    char buffer[AERON_MAX_PATH];
    const char *dirname = driver->context->aeron_dir;
    aeron_log_func_t log_func = aeron_log_func_none;

    if (stat(dirname, &sb) == 0 && S_ISDIR(sb.st_mode))
    {
        if (driver->context->warn_if_dirs_exist)
        {
            log_func = aeron_log_func_stderr;
            snprintf(buffer, sizeof(buffer) - 1, "WARNING: %s already exists", dirname);
            log_func(buffer);
        }

        if (driver->context->dirs_delete_on_start)
        {
            aeron_dir_delete(driver->context->aeron_dir);
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
                &cnc_mmap, driver->context->driver_timeout_ms, aeron_epoch_clock(), log_func))
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
            aeron_dir_delete(driver->context->aeron_dir);
        }
    }

    if (mkdir(driver->context->aeron_dir, S_IRWXU) != 0)
    {
        int errcode = errno;
        aeron_set_err(errcode, "mkdir %s: %s", driver->context->aeron_dir, strerror(errcode));
        return -1;
    }

    snprintf(buffer, sizeof(buffer) - 1, "%s/%s", dirname, AERON_PUBLICATIONS_DIR);
    if (mkdir(buffer, S_IRWXU) != 0)
    {
        int errcode = errno;
        aeron_set_err(errcode, "mkdir %s: %s", buffer, strerror(errcode));
        return -1;
    }

    snprintf(buffer, sizeof(buffer) - 1, "%s/%s", dirname, AERON_IMAGES_DIR);
    if (mkdir(buffer, S_IRWXU) != 0)
    {
        int errcode = errno;
        aeron_set_err(errcode, "mkdir %s: %s", buffer, strerror(errcode));
        return -1;
    }

    return 0;
}

void aeron_driver_fill_cnc_metadata(aeron_driver_context_t *context)
{
    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)context->cnc_map.addr;
    metadata->to_driver_buffer_length = (int32_t)context->to_driver_buffer_length;
    metadata->to_clients_buffer_length = (int32_t)context->to_clients_buffer_length;
    metadata->counter_metadata_buffer_length = (int32_t)context->counters_metadata_buffer_length;
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
    int result = -1, probe_fd;

    if ((probe_fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "socket %s:%d: %s", __FILE__, __LINE__, strerror(errcode));
        goto cleanup;
    }

    size_t default_sndbuf = 0;
    socklen_t len = sizeof(default_sndbuf);
    if (getsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &default_sndbuf, &len) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "getsockopt(SO_SNDBUF) %s:%d: %s", __FILE__, __LINE__, strerror(errcode));
        goto cleanup;
    }

    size_t default_rcvbuf = 0;
    len = sizeof(default_rcvbuf);
    if (getsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &default_sndbuf, &len) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "getsockopt(SO_RCVBUF) %s:%d: %s", __FILE__, __LINE__, strerror(errcode));
        goto cleanup;
    }

    size_t max_rcvbuf = default_rcvbuf;
    size_t max_sndbuf = default_sndbuf;

    if (driver->context->socket_sndbuf > 0)
    {
        size_t socket_sndbuf = driver->context->socket_sndbuf;

        if (setsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &socket_sndbuf, sizeof(socket_sndbuf)) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "setsockopt(SO_SNDBUF) %s:%d: %s", __FILE__, __LINE__, strerror(errcode));
            goto cleanup;
        }

        len = sizeof(socket_sndbuf);
        if (getsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &socket_sndbuf, &len) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "getsockopt(SO_SNDBUF) %s:%d: %s", __FILE__, __LINE__, strerror(errcode));
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

        if (setsockopt(probe_fd, SOL_SOCKET, SO_RCVBUF, &socket_rcvbuf, sizeof(socket_rcvbuf)) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "setsockopt(SO_RCVBUF) %s:%d: %s", __FILE__, __LINE__, strerror(errcode));
            goto cleanup;
        }

        len = sizeof(socket_rcvbuf);
        if (getsockopt(probe_fd, SOL_SOCKET, SO_RCVBUF, &socket_rcvbuf, &len) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "getsockopt(SO_RCVBUF) %s:%d: %s", __FILE__, __LINE__, strerror(errcode));
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
        close(probe_fd);

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
        int errcode = errno;

        aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
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

    if (aeron_driver_ensure_dir_is_recreated(_driver) < 0)
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

    if (aeron_driver_conductor_init(&_driver->conductor, context) < 0)
    {
        goto error;
    }

    _driver->context->conductor_proxy = &_driver->conductor.conductor_proxy;

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

    switch (_driver->context->threading_mode)
    {
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

    aeron_free(driver);
    return 0;
}
