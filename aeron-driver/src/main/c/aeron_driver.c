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
#include "aeronmd.h"
#include "aeron_alloc.h"

void aeron_log_func_stderr(const char *str)
{
    fprintf(stderr, "%s", str);
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

int aeron_report_existing_errors(const char *aeron_dir)
{
    return -1;
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
