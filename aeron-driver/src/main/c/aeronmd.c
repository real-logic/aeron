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

#define _GNU_SOURCE

#include <stdlib.h>
#include <signal.h>
#include <stdatomic.h>
#include <time.h>
#include <stdbool.h>
#include <stdio.h>
#include "aeronmd.h"

static atomic_bool running;

void sigint_handler(int signal)
{
    atomic_store(&running, false);
}

/**
 *  $ $0 <file and URL list>
 */
int main(int argc, char **argv)
{
    int status = EXIT_FAILURE;
    aeron_driver_context_t *context = NULL;
    aeron_driver_t *driver = NULL;

    atomic_init(&running, true);
    signal(SIGINT, sigint_handler);

    if (aeron_driver_context_init(&context) < 0)
    {
        goto cleanup;
    }

    if (aeron_driver_init(&driver, context) < 0)
    {
        goto cleanup;
    }

    if (aeron_driver_start(driver, true) < 0)
    {
        goto cleanup;
    }

    while (atomic_load(&running))
    {
        aeron_driver_main_idle_strategy(driver, aeron_driver_main_do_work(driver));
    }

    printf("Shutting down driver...\n");

    cleanup:

    aeron_driver_close(driver);
    aeron_driver_context_close(context);

    return status;
}
