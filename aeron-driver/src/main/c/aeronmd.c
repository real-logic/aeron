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

#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <stdbool.h>
#include <stdio.h>
#include <unistd.h>
#include "aeronmd.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_driver_context.h"
#include "util/aeron_properties_util.h"

volatile bool running = true;

void sigint_handler(int signal)
{
    AERON_PUT_ORDERED(running, false);
}

void termination_hook(void *state)
{
    AERON_PUT_ORDERED(running, false);
}

inline bool is_running()
{
    bool result;
    AERON_GET_VOLATILE(result, running);
    return result;
}

int set_property(void *clientd, const char *name, const char *value)
{
    return aeron_properties_setenv(name, value);
}

int main(int argc, char **argv)
{
    int status = EXIT_FAILURE;
    int opt;
    aeron_driver_context_t *context = NULL;
    aeron_driver_t *driver = NULL;

#ifndef _MSC_VER
    while ((opt = getopt(argc, argv, "D:")) != -1)
    {
        switch (opt)
        {
            case 'D':
            {
                aeron_properties_parser_state_t state;
                aeron_properties_parse_init(&state);
                if (aeron_properties_parse_line(&state, optarg, strlen(optarg), set_property, NULL) < 0)
                {
                    fprintf(stderr, "malformed define: %s\n", optarg);
                    exit(status);
                }
                break;
            }
            default:
                fprintf(stderr, "Usage: %s [-Dname=value]\n", argv[0]);
                exit(status);
        }
    }
#endif

    if (argc > optind)
    {
        if (aeron_properties_file_load(argv[optind]) < 0)
        {
            fprintf(stderr, "ERROR: loading properties file (%d) %s\n", aeron_errcode(), aeron_errmsg());
            exit(status);
        }
    }

    signal(SIGINT, sigint_handler);

    if (aeron_driver_context_init(&context) < 0)
    {
        fprintf(stderr, "ERROR: context init (%d) %s\n", aeron_errcode(), aeron_errmsg());
        goto cleanup;
    }

    context->termination_hook_func = termination_hook;

    if (aeron_driver_init(&driver, context) < 0)
    {
        fprintf(stderr, "ERROR: driver init (%d) %s\n", aeron_errcode(), aeron_errmsg());
        goto cleanup;
    }

    if (aeron_driver_start(driver, true) < 0)
    {
        fprintf(stderr, "ERROR: driver start (%d) %s\n", aeron_errcode(), aeron_errmsg());
        goto cleanup;
    }

    while (is_running())
    {
        aeron_driver_main_idle_strategy(driver, aeron_driver_main_do_work(driver));
    }

    printf("Shutting down driver...\n");

    cleanup:

    aeron_driver_close(driver);
    aeron_driver_context_close(context);

    return status;
}

extern bool is_running();
