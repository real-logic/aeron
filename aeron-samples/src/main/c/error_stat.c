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

#include <errno.h>
#include <stdio.h>
#include <inttypes.h>

#ifndef _MSC_VER
#include <unistd.h>
#include <getopt.h>
#endif

#include "aeronc.h"
#include "aeron_common.h"
#include "aeron_cnc_file_descriptor.h"
#include "concurrent/aeron_thread.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "util/aeron_strutil.h"
#include "util/aeron_error.h"

typedef struct aeron_error_stat_settings_stct
{
    const char *base_path;
    int64_t timeout_ms;
}
aeron_error_stat_settings_t;

static const char *aeron_error_stat_usage()
{
    return
        "    -h            Displays help information.\n"
        "    -d basePath   Base Path to shared memory. Default: /dev/shm/aeron-[user]\n"
        "    -t timeout    Number of milliseconds to wait to see if the driver metadata is available. Default: 1000\n";
}

static void aeron_error_stat_print_error_and_usage(const char *message)
{
    fprintf(stderr, "%s\n%s", message, aeron_error_stat_usage());
}

void aeron_error_stat_on_observation(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    char first_timestamp[AERON_MAX_PATH];
    char last_timestamp[AERON_MAX_PATH];

    aeron_format_date(first_timestamp, sizeof(first_timestamp), first_observation_timestamp);
    aeron_format_date(last_timestamp, sizeof(last_timestamp), last_observation_timestamp);

    fprintf(
        stdout,
        "***\n%d observations from %s to %s for:\n %.*s\n",
        observation_count,
        first_timestamp,
        last_timestamp,
        (int)error_length,
        error);
}


int main(int argc, char **argv)
{
    char default_directory[AERON_MAX_PATH];
    aeron_default_path(default_directory, AERON_MAX_PATH);
    aeron_error_stat_settings_t settings =
        {
            .base_path = default_directory,
            .timeout_ms = 1000
        };

    int opt;

    while ((opt = getopt(argc, argv, "d:t:h")) != -1)
    {
        switch (opt)
        {
            case 'd':
                settings.base_path = optarg;
                break;

            case 't':
            {
                errno = 0;
                char *endptr;
                settings.timeout_ms = strtoll(optarg, &endptr, 10);
                if (0 != errno || '\0' != endptr[0])
                {
                    aeron_error_stat_print_error_and_usage("Invalid timeout");
                    return EXIT_FAILURE;
                }
                break;
            }

            case 'h':
                aeron_error_stat_print_error_and_usage(argv[0]);
                return EXIT_SUCCESS;

            default:
                aeron_error_stat_print_error_and_usage("Unknown option");
                return EXIT_FAILURE;
        }
    }

    aeron_cnc_t *aeron_cnc = NULL;

    if (aeron_cnc_init(&aeron_cnc, settings.base_path, settings.timeout_ms) < 0)
    {
        aeron_error_stat_print_error_and_usage(aeron_errmsg());
        return EXIT_FAILURE;
    }

    size_t count = aeron_cnc_error_log_read(aeron_cnc, aeron_error_stat_on_observation, NULL, 0);

    fprintf(stdout, "\n%" PRIu64 " distinct errors observed.\n", (uint64_t)count);

    aeron_cnc_close(aeron_cnc);

    return 0;
}
