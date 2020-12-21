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

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <memory.h>
#include <errno.h>

#include "concurrent/aeron_thread.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"
#include "command/aeron_control_protocol.h"

#define AERON_ERR_TRAILER "...\n"

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

    if (NULL != error_state)
    {
        return error_state->errmsg;
    }
    else
    {
        return "no error";
    }
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

const char *aeron_error_code_str(int errcode)
{
    switch (errcode)
    {
        case AERON_ERROR_CODE_UNUSED:
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

static void aeron_err_vprintf(
    aeron_per_thread_error_t *error_state,
    const char *format,
    va_list args)
{
    if (error_state->offset >= sizeof(error_state->errmsg))
    {
        return;
    }

    int result = vsnprintf(
        &error_state->errmsg[(int)error_state->offset], sizeof(error_state->errmsg) - error_state->offset, format, args);

    if (result < 0)
    {
        fprintf(stderr, "Failed to update err_msg: %d\n", result);
    }

    error_state->offset += result;
}

static void aeron_err_printf(aeron_per_thread_error_t *error_state, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    aeron_err_vprintf(error_state, format, args);
    va_end(args);
}

static void aeron_err_update_entry(
    aeron_per_thread_error_t *error_state,
    const char *function,
    const char *filename,
    int line_number,
    const char *format,
    va_list args)
{
    aeron_err_printf(error_state, "[%s, %s:%d] ", function, filename, line_number);
    aeron_err_vprintf(error_state, format, args);
    aeron_err_printf(error_state, "%s", "\n");
    strcpy(error_state->errmsg + (sizeof(error_state->errmsg) - (strlen(AERON_ERR_TRAILER) + 2)), AERON_ERR_TRAILER);
}

void aeron_err_set(int errcode, const char *function, const char *filename, int line_number, const char *format, ...)
{
    aeron_per_thread_error_t *error_state = get_required_error_state();

    error_state->errcode = errcode;
    aeron_set_errno(errcode);
    error_state->offset = 0;

    const char *err_str = aeron_errcode() <= 0 ? aeron_error_code_str(-aeron_errcode()) : strerror(aeron_errcode());
    aeron_err_printf(error_state, "(%d) %s\n", aeron_errcode(), err_str);
    va_list args;
    va_start(args, format);
    aeron_err_update_entry(error_state, function, filename, line_number, format, args);
    va_end(args);
}

void aeron_err_append(const char *function, const char *filename, int line_number, const char *format, ...)
{
    aeron_per_thread_error_t *error_state = get_required_error_state();
    va_list args;
    va_start(args, format);
    aeron_err_update_entry(error_state, function, filename, line_number, format, args);
    va_end(args);
}

void aeron_err_clear()
{
    aeron_per_thread_error_t *error_state = get_required_error_state();

    aeron_set_errno(0);
    error_state->errcode = 0;
    strcpy(error_state->errmsg, "no error");
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
