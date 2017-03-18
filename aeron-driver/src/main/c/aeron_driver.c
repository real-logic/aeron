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

#include <stddef.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <inttypes.h>
#include "aeronmd.h"
#include "aeron_alloc.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "util/aeron_strutil.h"

void aeron_log_func_stderr(const char *str)
{
    fprintf(stderr, "%s\n", str);
}

void aeron_log_func_none(const char *str)
{
}

int64_t aeron_nanoclock()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) < 0)
    {
        return -1;
    }

    return (ts.tv_sec * 1000000000 + ts.tv_nsec);

}

int64_t aeron_epochclock()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) < 0)
    {
        return -1;
    }

    return (ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
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

int aeron_report_existing_errors(const char *aeron_dir)
{
    struct stat sb;
    char buffer[AERON_MAX_PATH];
    int fd, result = 0;

    if (stat(aeron_dir, &sb) == 0 && (S_ISDIR(sb.st_mode)))
    {
        snprintf(buffer, sizeof(buffer) - 1, "%s/%s", aeron_dir, AERON_CNC_FILE);
        if ((fd = open(buffer, O_RDONLY)) >= 0)
        {
            if (fstat(fd, &sb) == 0)
            {
                void *cnc_mmap = mmap(NULL, (size_t) sb.st_size, PROT_READ, MAP_FILE, fd, 0);

                if (MAP_FAILED != cnc_mmap)
                {
                    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *) cnc_mmap;

                    if (AERON_CNC_VERSION == metadata->cnc_version &&
                        aeron_error_log_exists(cnc_mmap, (size_t)sb.st_size))
                    {
                        char datestamp[AERON_MAX_PATH];
                        FILE *saved_errors_file = NULL;

                        aeron_format_date(datestamp, sizeof(datestamp) - 1, aeron_epochclock());
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
                        }
                        else
                        {
                            result = -1;
                        }

                        fclose(saved_errors_file);
                    }
                    else
                    {
                        result = -1;
                    }

                    munmap(cnc_mmap, (size_t) sb.st_size);
                }
                else
                {
                    result = -1;
                }
            }

            close(fd);
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

    if (stat(dirname, &sb) == 0 && (S_ISDIR(sb.st_mode)))
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
            if (aeron_is_driver_active(
                driver->context->aeron_dir, driver->context->driver_timeout_ms, aeron_epochclock(), log_func))
            {
                /* TODO: EINVAL? or ESTATE? */
                return -1;
            }

            if (aeron_report_existing_errors(driver->context->aeron_dir) < 0)
            {
                return -1;
            }

            aeron_dir_delete(driver->context->aeron_dir);
        }
    }

    if (mkdir(driver->context->aeron_dir, S_IRWXU) != 0)
    {
        /* TODO: report error */
        return -1;
    }

    return 0;
}

int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context)
{
    aeron_driver_t *_driver = NULL;

    if (NULL == driver || NULL == context)
    {
        /* TODO: EINVAL */
        return -1;
    }

    if (aeron_alloc((void **)&_driver, sizeof(aeron_driver_t)) < 0)
    {
        return -1;
    }

    _driver->context = context;

    if (aeron_driver_ensure_dir_is_recreated(_driver) < 0)
    {
        return -1;
    }

    *driver = _driver;
    return 0;
}

int aeron_driver_start(aeron_driver_t *driver)
{
    return 0;
}

int aeron_driver_close(aeron_driver_t *driver)
{
    if (NULL == driver)
    {
        /* TODO: EINVAL */
        return -1;
    }

    aeron_free(driver);
    return 0;
}
