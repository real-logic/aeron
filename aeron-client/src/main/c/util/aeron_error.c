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

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <memory.h>
#include <errno.h>

#include "concurrent/aeron_thread.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"
#include "command/aeron_control_protocol.h"

#if defined(AERON_COMPILER_MSVC)
#include <windows.h>
#endif

static AERON_INIT_ONCE error_is_initialized = AERON_INIT_ONCE_VALUE;

#if defined(AERON_COMPILER_MSVC)
static pthread_key_t error_key = TLS_OUT_OF_INDEXES;
#else
static pthread_key_t error_key;
#endif

static void initialize_per_thread_error()
{
    if (aeron_thread_key_create(&error_key, free))
    {
        fprintf(stderr, "could not create per thread error key, exiting.\n");
        exit(EXIT_FAILURE);
    }
}

static void initialize_error()
{
#if defined(AERON_COMPILER_MSVC)
    if (error_key != TLS_OUT_OF_INDEXES)
    {
        return;
    }
#endif

    aeron_thread_once(&error_is_initialized, initialize_per_thread_error);
}

int aeron_errcode()
{
    initialize_error();

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
    initialize_error();

    aeron_per_thread_error_t *error_state = aeron_thread_get_specific(error_key);
    const char *result = "";

    if (NULL != error_state)
    {
        result = error_state->errmsg;
    }

    return result;
}

static aeron_per_thread_error_t *get_required_error_state()
{
    initialize_error();

    aeron_per_thread_error_t *error_state = aeron_thread_get_specific(error_key);

    if (NULL == error_state)
    {
        if (aeron_alloc_no_err((void **)&error_state, sizeof(aeron_per_thread_error_t)) < 0)
        {
            fprintf(stderr, "could not create per thread error state, exiting.\n");
            exit(EXIT_FAILURE);
        }

        if (aeron_thread_set_specific(error_key, error_state))
        {
            fprintf(stderr, "could not associate per thread error key, exiting.\n");
            exit(EXIT_FAILURE);
        }
    }

    return error_state;
}

void aeron_set_err(int errcode, const char *format, ...)
{
    aeron_per_thread_error_t *error_state = get_required_error_state();

    error_state->errcode = errcode;
    aeron_set_errno(errcode);

    va_list args;
    char stack_message[sizeof(error_state->errmsg)];

    va_start(args, format);
    vsnprintf(stack_message, sizeof(stack_message) - 1, format, args);
    va_end(args);
    strncpy(error_state->errmsg, stack_message, sizeof(error_state->errmsg));
}

void aeron_set_errno(int errcode)
{
    errno = errcode;
#if defined(AERON_COMPILER_MSVC)
    switch (errcode)
    {
        case 0:
            SetLastError(ERROR_SUCCESS);
            break;

        case EINVAL:
            SetLastError(ERROR_BAD_ARGUMENTS);
            break;

        case ENOMEM:
            SetLastError(ERROR_OUTOFMEMORY);
            break;

        default:
            break;
    }
#endif
}

void aeron_set_err_from_last_err_code(const char *format, ...)
{
#if defined(AERON_COMPILER_MSVC)
    int errcode = (int)GetLastError();
#else
    int errcode = errno;
#endif

    aeron_per_thread_error_t *error_state = get_required_error_state();

    error_state->errcode = errcode;
    va_list args;
    char stack_message[sizeof(error_state->errmsg)];

    va_start(args, format);
    int written = vsnprintf(stack_message, sizeof(stack_message) - 1, format, args);
    va_end(args);

    if (written < 0)
    {
        error_state->errmsg[0] = '\0';
        return;
    }

    strncpy(error_state->errmsg, stack_message, sizeof(error_state->errmsg));

#if defined(AERON_COMPILER_MSVC)
    int length = (int)FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        errcode,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        stack_message,
        sizeof(stack_message),
        NULL);

    if (length >= 2 && stack_message[length - 1] == '\n' && stack_message[length - 2] == '\r')
    {
        stack_message[length - 2] = '\0';
    }
    else if (!length)
    {
        snprintf(stack_message, sizeof(stack_message), "error %d", errcode);
    }

    snprintf(error_state->errmsg + written, sizeof(error_state->errmsg) - written, ": %s", stack_message);
#else
    snprintf(error_state->errmsg + written, sizeof(error_state->errmsg) - written, ": %s", strerror(errcode));
#endif
}

const char *aeron_error_code_str(int errcode)
{
    switch (errcode)
    {
        case AERON_ERROR_CODE_GENERIC_ERROR:
            return "generic error, see message";

        case AERON_ERROR_CODE_INVALID_CHANNEL:
            return "invalid channel";

        case AERON_ERROR_CODE_UNKNOWN_SUBSCRIPTION:
            return "unknown subscription";

        case AERON_ERROR_CODE_UNKNOWN_PUBLICATION:
            return "unknown publication";

        case AERON_ERROR_CODE_CHANNEL_ENDPOINT_ERROR:
            return "channel endpoint error";

        case AERON_ERROR_CODE_UNKNOWN_COUNTER:
            return "unknown counter";

        case AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID:
            return "unknown command type id";

        case AERON_ERROR_CODE_MALFORMED_COMMAND:
            return "malformed command";

        case AERON_ERROR_CODE_NOT_SUPPORTED:
            return "not supported";

        case AERON_ERROR_CODE_UNKNOWN_HOST:
            return "unknown host";

        case AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE:
            return "resource temporarily unavailable";

        default:
            return "unknown error code";
    }
}

#if defined(AERON_COMPILER_MSVC)

bool aeron_error_dll_process_attach()
{
    if (error_key != TLS_OUT_OF_INDEXES)
    {
        return false;
    }

    error_key = TlsAlloc();

    return error_key != TLS_OUT_OF_INDEXES;
}

void aeron_error_dll_thread_detach()
{
    if (error_key == TLS_OUT_OF_INDEXES)
    {
        return;
    }

    aeron_per_thread_error_t *error_state = aeron_thread_get_specific(error_key);

    if (NULL != error_state)
    {
        aeron_free(error_state);
        aeron_thread_set_specific(error_key, NULL);
    }
}

void aeron_error_dll_process_detach()
{
    if (error_key == TLS_OUT_OF_INDEXES)
    {
        return;
    }

    aeron_error_dll_thread_detach();

    aeron_thread_key_delete(error_key);
    error_key = TLS_OUT_OF_INDEXES;
}

#endif
