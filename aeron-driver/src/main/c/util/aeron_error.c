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

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <memory.h>

#include "concurrent/aeron_thread.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"

static AERON_INIT_ONCE error_is_initialized = AERON_INIT_ONCE_VALUE;
static pthread_key_t error_key;

static void initialize_per_thread_error()
{
    if (aeron_thread_key_create(&error_key, free) < 0)
    {
        fprintf(stderr, "could not create per thread error key, exiting.\n");
        exit(EXIT_FAILURE);
    }
}

int aeron_errcode()
{
    (void) aeron_thread_once(&error_is_initialized, initialize_per_thread_error);
    aeron_per_thread_error_t *error_state = aeron_thread_get_specific(error_key);
    int result = 0;

    if (NULL != error_state)
    {
        result = error_state->errcode;
    }

    return result;
}

const char *aeron_errmsg()
{
    (void) aeron_thread_once(&error_is_initialized, initialize_per_thread_error);
    aeron_per_thread_error_t *error_state = aeron_thread_get_specific(error_key);
    const char *result = "";

    if (NULL != error_state)
    {
        result = error_state->errmsg;
    }

    return result;
}

void aeron_set_err(int errcode, const char *format, ...)
{
    (void) aeron_thread_once(&error_is_initialized, initialize_per_thread_error);
    aeron_per_thread_error_t *error_state = aeron_thread_get_specific(error_key);

    if (NULL == error_state)
    {
        if (aeron_alloc_no_err((void **)&error_state, sizeof(aeron_per_thread_error_t)) < 0)
        {
            fprintf(stderr, "could not create per thread error state, exiting.\n");
            exit(EXIT_FAILURE);
        }

        if (aeron_thread_set_specific(error_key, error_state) < 0)
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

#ifdef _MSC_VER
#include <WinSock2.h>
#include <windows.h>

void aeron_set_windows_error()
{
    const DWORD errorId = GetLastError();
    LPSTR messageBuffer = NULL;

    FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        errorId,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&messageBuffer,
        0,
        NULL);

    aeron_set_err(errorId, messageBuffer);
    free(messageBuffer);
}
#endif
