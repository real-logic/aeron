/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <memory.h>

#if !defined(_MSC_VER)
#include <pthread.h>

#else
/* Win32 Threads */
#endif

#include "util/aeron_error.h"
#include "aeron_alloc.h"

static pthread_once_t error_is_initialized = PTHREAD_ONCE_INIT;
static pthread_key_t error_key;

static void initialize_per_thread_error()
{
    if (pthread_key_create(&error_key, free) < 0)
    {
        fprintf(stderr, "could not create per thread error key, exiting.\n");
        exit(EXIT_FAILURE);
    }
}

int aeron_errcode()
{
    (void) pthread_once(&error_is_initialized, initialize_per_thread_error);
    aeron_per_thread_error_t *error_state = pthread_getspecific(error_key);
    int result = 0;

    if (NULL != error_state)
    {
        result = error_state->errcode;
    }

    return result;
}

const char *aeron_errmsg()
{
    (void) pthread_once(&error_is_initialized, initialize_per_thread_error);
    aeron_per_thread_error_t *error_state = pthread_getspecific(error_key);
    const char *result = "";

    if (NULL != error_state)
    {
        result = error_state->errmsg;
    }

    return result;
}

void aeron_set_err(int errcode, const char *format, ...)
{
    (void) pthread_once(&error_is_initialized, initialize_per_thread_error);
    aeron_per_thread_error_t *error_state = pthread_getspecific(error_key);

    if (NULL == error_state)
    {
        if (aeron_alloc_no_err((void **)&error_state, sizeof(aeron_per_thread_error_t)) < 0)
        {
            fprintf(stderr, "could not create per thread error state, exiting.\n");
            exit(EXIT_FAILURE);
        }

        if (pthread_setspecific(error_key, error_state) < 0)
        {
            fprintf(stderr, "could not associate per thread error key, exiting.\n");
            exit(EXIT_FAILURE);
        }
    }

    error_state->errcode = errcode;
    va_list args;
    char stack_message[sizeof(error_state->errmsg)];

    va_start(args, format);
    vsnprintf(stack_message, sizeof(stack_message) - 1, format, args);
    va_end(args);
    strncpy(error_state->errmsg, stack_message, sizeof(error_state->errmsg) - 1);
}
