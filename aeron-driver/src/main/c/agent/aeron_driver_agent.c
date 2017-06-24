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

#if !defined(_MSC_VER)
#include <pthread.h>
#else
/* Win32 Threads */
#endif

#include "util/aeron_error.h"
#include "aeronmd.h"
#include "command/aeron_control_protocol.h"
#include "protocol/aeron_udp_protocol.h"

static pthread_once_t agent_is_initialized = PTHREAD_ONCE_INIT;

#include <stdio.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include "agent/aeron_driver_agent.h"

#define AERON_INTERCEPT_FUNC_RETURN_ON_ERROR(type,funcvar) \
do \
{ \
    if (NULL == funcvar) \
    { \
        if ((funcvar = (type ## _t)dlsym(RTLD_NEXT, #type)) == NULL) \
        { \
            fprintf(stderr, "could not hook func <%s>: %s\n", #type, dlerror()); \
            return; \
        } \
    } \
} \
while(0)

static aeron_mpsc_rb_t logging_mpsc_rb;
static uint8_t *rb_buffer = NULL;
static uint64_t mask = 0;
static pthread_t log_reader_thread;

static void *aeron_driver_agent_log_reader(void *arg)
{
    while (true)
    {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000 * 1000 };

        aeron_mpsc_rb_read(&logging_mpsc_rb, aeron_driver_agent_log_dissector, NULL, 10);
        nanosleep(&ts, NULL);
    }
}

static void initialize_agent_logging()
{
    char *mask_str = getenv(AERON_AGENT_MASK_ENV_VAR);

    if (mask_str)
    {
        mask = strtoull(mask_str, NULL, 0);
    }

    if (mask != 0)
    {
        size_t rb_length = RING_BUFFER_LENGTH + AERON_RB_TRAILER_LENGTH;
        if ((rb_buffer = (uint8_t *) malloc(rb_length)) == NULL)
        {
            fprintf(stderr, "could not allocate ring buffer buffer. exiting.\n");
            exit(EXIT_FAILURE);
        }

        if (aeron_mpsc_rb_init(&logging_mpsc_rb, rb_buffer, rb_length) < 0)
        {
            fprintf(stderr, "could not init logging mpwc_rb. exiting.\n");
            exit(EXIT_FAILURE);
        }

        if (pthread_create(&log_reader_thread, NULL, aeron_driver_agent_log_reader, NULL) != 0)
        {
            fprintf(stderr, "could not start log reader thread. exiting.\n");
            exit(EXIT_FAILURE);
        }
    }
}

void aeron_driver_conductor_on_command(int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    static aeron_driver_conductor_on_command_t _original_func = NULL;

    (void) pthread_once(&agent_is_initialized, initialize_agent_logging);

    AERON_INTERCEPT_FUNC_RETURN_ON_ERROR(aeron_driver_conductor_on_command, _original_func);

    uint8_t buffer[MAX_CMD_LENGTH + sizeof(aeron_driver_agent_cmd_log_header_t)];
    aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
    hdr->time_ms = aeron_epochclock();
    hdr->cmd_id = msg_type_id;
    memcpy(buffer + sizeof(aeron_driver_agent_cmd_log_header_t), message, length);

    aeron_mpsc_rb_write(&logging_mpsc_rb, AERON_CMD_IN, buffer, length + sizeof(aeron_driver_agent_cmd_log_header_t));

    _original_func(msg_type_id, message, length, clientd);
}

void aeron_driver_conductor_client_transmit(
    aeron_driver_conductor_t *conductor,
    int32_t msg_type_id,
    const void *message,
    size_t length)
{
    static aeron_driver_conductor_client_transmit_t _original_func = NULL;

    (void) pthread_once(&agent_is_initialized, initialize_agent_logging);

    AERON_INTERCEPT_FUNC_RETURN_ON_ERROR(aeron_driver_conductor_client_transmit, _original_func);

    uint8_t buffer[MAX_CMD_LENGTH + sizeof(aeron_driver_agent_cmd_log_header_t)];
    aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
    hdr->time_ms = aeron_epochclock();
    hdr->cmd_id = msg_type_id;
    memcpy(buffer + sizeof(aeron_driver_agent_cmd_log_header_t), message, length);

    aeron_mpsc_rb_write(&logging_mpsc_rb, AERON_CMD_OUT, buffer, length + sizeof(aeron_driver_agent_cmd_log_header_t));

    _original_func(conductor, msg_type_id, message, length);
}

static const char *dissect_msg_type_id(int32_t id)
{
    switch (id)
    {
        case AERON_CMD_IN:
            return "CMD_IN";
        case AERON_CMD_OUT:
            return "CMD_OUT";
        case AERON_FRAME_IN:
            return "FRAME_IN";
        case AERON_FRAME_OUT:
            return "FRAME_OUT";
        default:
            return "unknown";
    }
}

static const char *dissect_timestamp(int64_t time_ms)
{
    static char buffer[256];

    snprintf(buffer, sizeof(buffer) - 1, "%" PRId64 ".%" PRId64, time_ms / 1000, time_ms % 1000);
    return buffer;
}

static const char *dissect_cmd_in(int64_t cmd_id, const void *message, size_t length)
{
    static char buffer[256];

    buffer[0] = '\0';
    switch (cmd_id)
    {
        case AERON_COMMAND_ADD_PUBLICATION:
        case AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION:
        {
            aeron_publication_command_t *command = (aeron_publication_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_publication_command_t);
            snprintf(buffer, sizeof(buffer) - 1, "%s %d %*s [%" PRId64 ":%" PRId64 "]",
                (cmd_id == AERON_COMMAND_ADD_PUBLICATION) ? "ADD_PUBLICATION" : "ADD_EXCLUSIVE_PUBLCIATION",
                command->stream_id,
                command->channel_length,
                channel,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_REMOVE_PUBLICATION:
        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
        {
            aeron_remove_command_t *command = (aeron_remove_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s %" PRId64 " [%" PRId64 ":%" PRId64 "]",
                (cmd_id == AERON_COMMAND_REMOVE_PUBLICATION) ? "REMOVE_PUBLICATION" : "REMOVE_SUBSCRIPTION",
                command->registration_id,
                command->correlated.client_id,
                command->correlated.correlation_id);

            break;
        }

        case AERON_COMMAND_ADD_SUBSCRIPTION:
        {
            aeron_subscription_command_t *command = (aeron_subscription_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_publication_command_t);
            snprintf(buffer, sizeof(buffer) - 1, "ADD_SUBSCRIPTION %d %*s [%" PRId64 "][%" PRId64 ":%" PRId64 "]",
                command->stream_id,
                command->channel_length,
                channel,
                command->registration_correlation_id,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_CLIENT_KEEPALIVE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "CLIENT_KEEPALIVE [%" PRId64 ":%" PRId64 "]",
                command->client_id,
                command->correlation_id);
            break;
        }

        default:
            break;
    }

    return buffer;
}

static const char *dissect_cmd_out(int64_t cmd_id, const void *message, size_t length)
{
    static char buffer[256];

    buffer[0] = '\0';
    switch (cmd_id)
    {
        case AERON_RESPONSE_ON_OPERATION_SUCCESS:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "ON_OPERATION_SUCCESS [%" PRId64 ":%" PRId64 "]",
                command->client_id,
                command->correlation_id);
            break;
        }

        case AERON_RESPONSE_ON_PUBLICATION_READY:
        case AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY:
        {
            aeron_publication_buffers_ready_t *command = (aeron_publication_buffers_ready_t *)message;

            const char *log_file_name = (const char *)message + sizeof(aeron_publication_buffers_ready_t);
            snprintf(buffer, sizeof(buffer) - 1, "%s %d:%d %d [%" PRId64 " %" PRId64 "]\n    %*s",
                (cmd_id == AERON_RESPONSE_ON_PUBLICATION_READY) ? "ON_PUBLICATION_READY" : "ON_EXCLUSIVE_PUBLICATION_READY",
                command->session_id,
                command->stream_id,
                command->position_limit_counter_id,
                command->correlation_id,
                command->registration_id,
                command->log_file_length,
                log_file_name);
            break;
        }

        case AERON_RESPONSE_ON_ERROR:
        {
            aeron_error_response_t *command = (aeron_error_response_t *)message;

            const char *error_message = (const char *)message + sizeof(aeron_error_response_t);
            snprintf(buffer, sizeof(buffer) - 1, "ON_ERROR %" PRId64 "%d %*s",
                command->offending_command_correlation_id,
                command->error_code,
                command->error_message_length,
                error_message);
            break;
        }

        case AERON_RESPONSE_ON_UNAVAILABLE_IMAGE:
        {
            aeron_image_message_t *command = (aeron_image_message_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_image_message_t);
            snprintf(buffer, sizeof(buffer) - 1, "ON_UNAVAILABLE_IMAGE %d %*s [%" PRId64 "]",
                command->stream_id,
                command->channel_length,
                channel,
                command->correlation_id);
            break;
        }

        case AERON_RESPONSE_ON_AVAILABLE_IMAGE:
        {
            aeron_image_buffers_ready_t *command = (aeron_image_buffers_ready_t *)message;
            char *ptr = buffer;
            int len = 0;

            char *positions = (char *)message + sizeof(aeron_image_buffers_ready_t);
            len = snprintf(buffer, sizeof(buffer) - 1, "ON_AVAILABLE_IMAGE %d:%d ",
                command->session_id,
                command->stream_id);

            aeron_image_buffers_ready_subscriber_position_t *position =
                (aeron_image_buffers_ready_subscriber_position_t *)positions;

            for (int32_t i = 0; i < command->subscriber_position_count; i++)
            {
                len += snprintf(ptr + len, sizeof(buffer) - 1 - len, "[%" PRId32 ":%" PRId32 ":%" PRId64 "]",
                    i, position[i].indicator_id, position[i].registration_id);
            }

            char *source_identity_ptr =
                positions + command->subscriber_position_count * sizeof(aeron_image_buffers_ready_subscriber_position_t);
            int32_t *source_identity_length = (int32_t *)source_identity_ptr;
            const char *source_identity = source_identity_ptr + sizeof(int32_t);
            len += snprintf(ptr + len, sizeof(buffer) - 1 - len, "\"%*s\" [%" PRId64 "]",
                *source_identity_length, source_identity, command->correlation_id);

            char *log_file_name_ptr = source_identity_ptr + *source_identity_length + sizeof(int32_t);
            int32_t *log_file_name_length = (int32_t *)log_file_name_ptr;
            const char *log_file_name = log_file_name_ptr + sizeof(int32_t);

            len += snprintf(ptr + len, sizeof(buffer) - 1 - len, "%*s", *log_file_name_length, log_file_name);
            break;
        }

        default:
            break;
    }

    return buffer;
}

void aeron_driver_agent_log_dissector(int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    switch (msg_type_id)
    {
        case AERON_CMD_OUT:
        {
            aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)message;

            printf(
                "[%s] %s %s\n",
                dissect_timestamp(hdr->time_ms),
                dissect_msg_type_id(msg_type_id),
                dissect_cmd_out(hdr->cmd_id, message, length));
            break;
        }

        case AERON_CMD_IN:
        {
            aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)message;

            printf(
                "[%s] %s %s\n",
                dissect_timestamp(hdr->time_ms),
                dissect_msg_type_id(msg_type_id),
                dissect_cmd_in(hdr->cmd_id, message, length));
            break;
        }

        case AERON_FRAME_IN:
        case AERON_FRAME_OUT:
        {
            break;
        }

        default:
            break;
    }
}

