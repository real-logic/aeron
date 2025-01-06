/*
 * Copyright 2014-2025 Real Logic Limited.
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
#include "util/aeron_strutil.h"

typedef struct aeron_driver_tool_settings_stct
{
    const char *base_path;
    bool pid_only;
    bool terminate_driver;
    long long timeout_ms;
}
aeron_driver_tool_settings_t;

static const char *aeron_driver_tool_usage(void)
{
    return
        "    -P            Print PID only without anything else.\n"
        "    -T            Request driver to terminate.\n"
        "    -d basePath   Base Path to shared memory. Default: /dev/shm/aeron-[user]\n"
        "    -h            Displays help information.\n"
        "    -t timeout    Number of milliseconds to wait to see if the driver metadata is available. Default: 1000\n";
}

static void aeron_driver_tool_print_error_and_usage(const char *message)
{
    fprintf(stderr, "%s\n%s", message, aeron_driver_tool_usage());
}

int main(int argc, char **argv)
{
    char default_directory[AERON_MAX_PATH];
    if (aeron_default_path(default_directory, sizeof(default_directory)) < 0)
    {
        aeron_driver_tool_print_error_and_usage("failed to resolve aeron directory path");
        return EXIT_FAILURE;
    }

    aeron_driver_tool_settings_t settings =
        {
            .base_path = default_directory,
            .pid_only = false,
            .terminate_driver = false,
            .timeout_ms = 1000
        };


    int opt;

    while ((opt = getopt(argc, argv, "d:t:PTh")) != -1)
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
                    aeron_driver_tool_print_error_and_usage("Invalid timeout");
                    return EXIT_FAILURE;
                }
                break;
            }

            case 'P':
                settings.pid_only = true;
                break;

            case 'T':
                settings.terminate_driver = true;
                break;

            case 'h':
                aeron_driver_tool_print_error_and_usage(argv[0]);
                return EXIT_SUCCESS;

            default:
                aeron_driver_tool_print_error_and_usage("Unknown option");
                return EXIT_FAILURE;
        }
    }

    aeron_cnc_t *aeron_cnc = NULL;
    aeron_cnc_constants_t cnc_constants;

    if (aeron_cnc_init(&aeron_cnc, settings.base_path, settings.timeout_ms) < 0)
    {
        aeron_driver_tool_print_error_and_usage(aeron_errmsg());
        return EXIT_FAILURE;
    }

    aeron_cnc_constants(aeron_cnc, &cnc_constants);

    if (settings.pid_only)
    {
        printf("%" PRId64 "\n", cnc_constants.pid);
    }
    else if (settings.terminate_driver)
    {
        aeron_context_request_driver_termination(settings.base_path, NULL, 0);
    }
    else
    {
        const char *cnc_filename = aeron_cnc_filename(aeron_cnc);
        int64_t now_ms = aeron_epoch_clock();
        const int64_t heartbeat_ms = aeron_cnc_to_driver_heartbeat(aeron_cnc);

        char now_timestamp_buffer[AERON_FORMAT_DATE_MAX_LENGTH];
        char start_timestamp_buffer[AERON_FORMAT_DATE_MAX_LENGTH];
        char heartbeat_timestamp_buffer[AERON_FORMAT_DATE_MAX_LENGTH];
        aeron_format_date(now_timestamp_buffer, sizeof(now_timestamp_buffer), now_ms);
        aeron_format_date(start_timestamp_buffer, sizeof(start_timestamp_buffer), cnc_constants.start_timestamp);
        aeron_format_date(heartbeat_timestamp_buffer, sizeof(heartbeat_timestamp_buffer), heartbeat_ms);

        fprintf(stdout, "Command 'n Control cnc_file: %s\n", cnc_filename);
        fprintf(stdout, "Version: %" PRId32 ", PID: %" PRId64 "\n", cnc_constants.cnc_version, cnc_constants.pid);
        fprintf(
            stdout,
            "%s (start: %s, activity: %s)\n",
            now_timestamp_buffer,
            start_timestamp_buffer,
            heartbeat_timestamp_buffer);
    }

    aeron_cnc_close(aeron_cnc);

    return EXIT_SUCCESS;
}
