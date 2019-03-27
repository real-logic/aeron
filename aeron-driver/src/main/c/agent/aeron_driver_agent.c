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

#if !defined(_MSC_VER)
#include <pthread.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#else
#endif


#include <stdio.h>
#include <stdlib.h>

#include <inttypes.h>
#include <stdarg.h>
#include "agent/aeron_driver_agent.h"
#include "aeron_driver_context.h"
#include "aeron_driver_agent.h"
#include "util/aeron_dlopen.h"
#include "concurrent/aeron_thread.h"
#include "aeron_windows.h"

static AERON_INIT_ONCE agent_is_initialized = AERON_INIT_ONCE_VALUE;
static aeron_mpsc_rb_t logging_mpsc_rb;
static uint8_t *rb_buffer = NULL;
static uint64_t mask = 0;
static double receive_data_loss_rate = 0.0;
static unsigned short receive_data_loss_xsubi[3];
static aeron_thread_t log_reader_thread;

int64_t aeron_agent_epoch_clock()
{
    struct timespec ts;
#if defined(AERON_COMPILER_MSVC)
    if (aeron_clock_gettime_realtime(&ts) < 0)
    {
        return -1;
    }
#else
    if (clock_gettime(CLOCK_REALTIME, &ts) < 0)
    {
        return -1;
    }
#endif
    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

bool aeron_agent_should_drop_frame(struct msghdr *message)
{
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)(message->msg_iov->iov_base);

    if (frame_header->type == AERON_HDR_TYPE_DATA || frame_header->type == AERON_HDR_TYPE_PAD)
    {
        return aeron_erand48(receive_data_loss_xsubi) <= receive_data_loss_rate;
    }

    return false;
}

static void *aeron_driver_agent_log_reader(void *arg)
{
    while (true)
    {
        aeron_mpsc_rb_read(&logging_mpsc_rb, aeron_driver_agent_log_dissector, NULL, 10);
        aeron_nano_sleep(1000 * 1000);
    }

    return NULL;
}

static void initialize_agent_logging()
{
    char *mask_str = getenv(AERON_AGENT_MASK_ENV_VAR);
    char *receive_loss_rate_str = getenv(AERON_AGENT_RECEIVE_DATA_LOSS_RATE_ENV_VAR);

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
        memset(rb_buffer, 0, rb_length);

        if (aeron_mpsc_rb_init(&logging_mpsc_rb, rb_buffer, rb_length) < 0)
        {
            fprintf(stderr, "could not init logging mpwc_rb. exiting.\n");
            exit(EXIT_FAILURE);
        }

        if (aeron_thread_create(&log_reader_thread, NULL, aeron_driver_agent_log_reader, NULL) != 0)
        {
            fprintf(stderr, "could not start log reader thread. exiting.\n");
            exit(EXIT_FAILURE);
        }
    }

    if (receive_loss_rate_str)
    {
        receive_data_loss_rate = strtod(receive_loss_rate_str, NULL);
    }

    if (0.0 != receive_data_loss_rate)
    {
        char *receive_loss_seed_str = getenv(AERON_AGENT_RECEIVE_DATA_LOSS_SEED_ENV_VAR);
        long long seed_value;

        if (receive_loss_seed_str)
        {
            seed_value = strtoll(receive_loss_seed_str, NULL, 0);
        }
        else
        {
            seed_value = aeron_agent_epoch_clock();
        }

        receive_data_loss_xsubi[2] = (unsigned short)(seed_value & 0xFFFF);
        receive_data_loss_xsubi[1] = (unsigned short)((seed_value >> 16) & 0xFFFF);
        receive_data_loss_xsubi[0] = (unsigned short)((seed_value >> 32) & 0xFFFF);
    }
}

void aeron_driver_agent_conductor_to_driver_interceptor(
    int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    uint8_t buffer[MAX_CMD_LENGTH + sizeof(aeron_driver_agent_cmd_log_header_t)];
    aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
    hdr->time_ms = aeron_agent_epoch_clock();
    hdr->cmd_id = msg_type_id;
    memcpy(buffer + sizeof(aeron_driver_agent_cmd_log_header_t), message, length);

    aeron_mpsc_rb_write(&logging_mpsc_rb, AERON_CMD_IN, buffer, length + sizeof(aeron_driver_agent_cmd_log_header_t));
}

void aeron_driver_agent_conductor_to_client_interceptor(
    aeron_driver_conductor_t *conductor, int32_t msg_type_id, const void *message, size_t length)
{
    uint8_t buffer[MAX_CMD_LENGTH + sizeof(aeron_driver_agent_cmd_log_header_t)];
    aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
    hdr->time_ms = aeron_agent_epoch_clock();
    hdr->cmd_id = msg_type_id;
    memcpy(buffer + sizeof(aeron_driver_agent_cmd_log_header_t), message, length);

    aeron_mpsc_rb_write(&logging_mpsc_rb, AERON_CMD_OUT, buffer, length + sizeof(aeron_driver_agent_cmd_log_header_t));
}

int aeron_driver_agent_map_raw_log_interceptor(
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size)
{
    int result = aeron_map_raw_log(mapped_raw_log, path, use_sparse_files, term_length, page_size);

    uint8_t buffer[AERON_MAX_PATH + sizeof(aeron_driver_agent_map_raw_log_op_header_t)];
    aeron_driver_agent_map_raw_log_op_header_t *hdr = (aeron_driver_agent_map_raw_log_op_header_t *)buffer;
    size_t path_len = strlen(path);

    hdr->time_ms = aeron_agent_epoch_clock();
    hdr->map_raw_log.path_len = (int32_t)path_len;
    hdr->map_raw_log.result = result;
    hdr->map_raw_log.addr = (uintptr_t)mapped_raw_log;
    memcpy(&hdr->map_raw_log.log, mapped_raw_log, sizeof(hdr->map_raw_log.log));
    memcpy(buffer + sizeof(aeron_driver_agent_map_raw_log_op_header_t), path, path_len);

    aeron_mpsc_rb_write(
        &logging_mpsc_rb, AERON_MAP_RAW_LOG_OP, buffer, sizeof(aeron_driver_agent_map_raw_log_op_header_t) + path_len);

    return result;
}

int aeron_driver_agent_map_raw_log_close_interceptor(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename)
{
    uint8_t buffer[AERON_MAX_PATH + sizeof(aeron_driver_agent_map_raw_log_op_header_t)];
    aeron_driver_agent_map_raw_log_op_header_t *hdr = (aeron_driver_agent_map_raw_log_op_header_t *)buffer;

    hdr->time_ms = aeron_agent_epoch_clock();
    hdr->map_raw_log_close.addr = (uintptr_t)mapped_raw_log;
    memcpy(&hdr->map_raw_log_close.log, mapped_raw_log, sizeof(hdr->map_raw_log.log));
    hdr->map_raw_log_close.result = aeron_map_raw_log_close(mapped_raw_log, filename);

    aeron_mpsc_rb_write(
        &logging_mpsc_rb, AERON_MAP_RAW_LOG_OP_CLOSE, buffer, sizeof(aeron_driver_agent_map_raw_log_op_header_t));

    return hdr->map_raw_log_close.result;
}

static void *aeron_lib = NULL;

int aeron_driver_context_init(aeron_driver_context_t **context)
{
    static aeron_driver_context_init_t _original_func = NULL;

#if defined(Darwin)
    static const char *aeron_driver_libname = "libaeron_driver.dylib";
#elif defined(__linux__)
    static const char *aeron_driver_libname = "libaeron_driver.so";
#elif defined(WINVER)
    static const char *aeron_driver_libname = "aeron_driver.dll";
#endif

    if (NULL == _original_func)
    {
        if ((aeron_lib = aeron_dlopen(aeron_driver_libname)) == NULL)
        {
            fprintf(stderr, "%s\n", aeron_dlerror());
            exit(EXIT_FAILURE);
        }

        if ((_original_func = (aeron_driver_context_init_t)aeron_dlsym(aeron_lib, "aeron_driver_context_init")) == NULL)
        {
            fprintf(stderr, "%s\n", aeron_dlerror());
            exit(EXIT_FAILURE);
        }

        printf("hooked aeron_driver_context_init\n");
    }

    (void)aeron_thread_once(&agent_is_initialized, initialize_agent_logging);

    int result = _original_func(context);

    if (mask & AERON_CMD_IN)
    {
        (*context)->to_driver_interceptor_func = aeron_driver_agent_conductor_to_driver_interceptor;
    }

    if (mask & AERON_CMD_OUT)
    {
        (*context)->to_client_interceptor_func = aeron_driver_agent_conductor_to_client_interceptor;
    }

    if (mask & AERON_MAP_RAW_LOG_OP)
    {
        (*context)->map_raw_log_func = aeron_driver_agent_map_raw_log_interceptor;
        (*context)->map_raw_log_close_func = aeron_driver_agent_map_raw_log_close_interceptor;
    }

    return result;
}

void aeron_driver_agent_log_frame(
    int32_t msg_type_id, int sockfd, const struct msghdr *msghdr, int flags, int result, int32_t message_len)
{
    uint8_t buffer[MAX_FRAME_LENGTH + sizeof(aeron_driver_agent_frame_log_header_t) + sizeof(struct sockaddr_in6)];
    aeron_driver_agent_frame_log_header_t *hdr = (aeron_driver_agent_frame_log_header_t *)buffer;
    size_t length = sizeof(aeron_driver_agent_frame_log_header_t);

    hdr->time_ms = aeron_agent_epoch_clock();
    hdr->result = (int32_t)result;
    hdr->sockaddr_len = msghdr->msg_namelen;
    hdr->message_len = message_len;

    if (msghdr->msg_iovlen > 1)
    {
        fprintf(stderr, "only aware of 1 iov. %d iovs detected.\n", (int)msghdr->msg_iovlen);
    }

    uint8_t *ptr = buffer + sizeof(aeron_driver_agent_frame_log_header_t);
    memcpy(ptr, msghdr->msg_name, msghdr->msg_namelen);
    length += msghdr->msg_namelen;

    ptr += msghdr->msg_namelen;
    int32_t copy_length = message_len < MAX_FRAME_LENGTH ? message_len : MAX_FRAME_LENGTH;
    memcpy(ptr, msghdr->msg_iov[0].iov_base, copy_length);
    length += copy_length;

    aeron_mpsc_rb_write(&logging_mpsc_rb, msg_type_id, buffer, (size_t)length);
}

typedef ssize_t (*aeron_driver_agent_sendmsg_func_t)(int socket, const struct msghdr *message, int flags);
typedef ssize_t (*aeron_driver_agent_recvmsg_func_t)(int socket, struct msghdr *message, int flags);

ssize_t sendmsg(int socket, const struct msghdr *message, int flags)
{
    static aeron_driver_agent_sendmsg_func_t _original_func = NULL;

    if (NULL == _original_func)
    {
        if ((_original_func = (aeron_driver_agent_sendmsg_func_t)aeron_dlsym(RTLD_NEXT, "sendmsg")) == NULL)
        {
            fprintf(stderr, "%s\n", aeron_dlerror());
            exit(EXIT_FAILURE);
        }

        printf("hooked sendmsg\n");
    }

    (void)aeron_thread_once(&agent_is_initialized, initialize_agent_logging);

    ssize_t result = _original_func(socket, message, flags);

    if (mask & AERON_FRAME_OUT)
    {
        aeron_driver_agent_log_frame(
            AERON_FRAME_OUT, socket, message, flags, (int)result, (int32_t)message->msg_iov[0].iov_len);
    }
    return result;
}

ssize_t recvmsg(int socket, struct msghdr *message, int flags)
{
    static aeron_driver_agent_recvmsg_func_t _original_func = NULL;

    if (NULL == _original_func)
    {
        if ((_original_func = (aeron_driver_agent_recvmsg_func_t)aeron_dlsym(RTLD_NEXT, "recvmsg")) == NULL)
        {
            fprintf(stderr, "%s\n", aeron_dlerror());
            exit(EXIT_FAILURE);
        }

        printf("hooked recvmsg\n");
    }

    (void)aeron_thread_once(&agent_is_initialized, initialize_agent_logging);

    ssize_t result = _original_func(socket, message, flags);

    if (result > 0)
    {
        if (receive_data_loss_rate > 0.0 && aeron_agent_should_drop_frame(message))
        {
            aeron_driver_agent_log_frame(AERON_FRAME_IN_DROPPED, socket, message, flags, (int)result, (int32_t)result);
            result = 0;
        }
        else if (mask & AERON_FRAME_IN)
        {
            aeron_driver_agent_log_frame(AERON_FRAME_IN, socket, message, flags, (int)result, (int32_t)result);
        }
    }

    return result;
}

#if defined(HAVE_SENDMMSG)

typedef int (*aeron_driver_agent_sendmmsg_func_t)
    (int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags);

int sendmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags)
{
    static aeron_driver_agent_sendmmsg_func_t _original_func = NULL;

    if (NULL == _original_func)
    {
        if ((_original_func = (aeron_driver_agent_sendmmsg_func_t)aeron_dlsym(RTLD_NEXT, "sendmmsg")) == NULL)
        {
            fprintf(stderr, "%s\n", aeron_dlerror());
            exit(EXIT_FAILURE);
        }

        printf("hooked sendmmsg\n");
    }

    (void) aeron_thread_once(&agent_is_initialized, initialize_agent_logging);

    int result = _original_func(sockfd, msgvec, vlen, flags);

    if (mask & AERON_FRAME_OUT)
    {
        for (int i = 0; i < result; i++)
        {
            aeron_driver_agent_log_frame(
                AERON_FRAME_OUT,
                sockfd,
                &msgvec[i].msg_hdr,
                flags,
                msgvec[i].msg_len,
                msgvec[i].msg_hdr.msg_iov[0].iov_len);
        }
    }

    return result;
}
#endif

#if defined(HAVE_RECVMMSG)

/* See: https://sourceware.org/bugzilla/show_bug.cgi?id=16852 */
#if __GLIBC__ <= 5 && __GLIBC_MINOR__ < 21
typedef const struct timespec * recvmmsg_timespec_ptr_t;
#else
typedef struct timespec * recvmmsg_timespec_ptr_t;
#endif

typedef int (*aeron_driver_agent_recvmmsg_func_t)
    (int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags, recvmmsg_timespec_ptr_t timeout);

int recvmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags, recvmmsg_timespec_ptr_t timeout)
{
    static aeron_driver_agent_recvmmsg_func_t _original_func = NULL;

    if (NULL == _original_func)
    {
        if ((_original_func = (aeron_driver_agent_recvmmsg_func_t)aeron_dlsym(RTLD_NEXT, "recvmmsg")) == NULL)
        {
            fprintf(stderr, "%s\n", aeron_dlerror());
            exit(EXIT_FAILURE);
        }

        printf("hooked recvmmsg\n");
    }

    (void)aeron_thread_once(&agent_is_initialized, initialize_agent_logging);

    int result = _original_func(sockfd, msgvec, vlen, flags, timeout);

    for (int i = 0; i < result; i++)
    {
        if (receive_data_loss_rate > 0.0 && aeron_agent_should_drop_frame(&msgvec[i].msg_hdr))
        {
            aeron_driver_agent_log_frame(
                AERON_FRAME_IN_DROPPED, sockfd, &msgvec[i].msg_hdr, flags, msgvec[i].msg_len, msgvec[i].msg_len);
            result = i;
            break;
        }
        else if (mask & AERON_FRAME_IN)
        {
            aeron_driver_agent_log_frame(
                AERON_FRAME_IN, sockfd, &msgvec[i].msg_hdr, flags, msgvec[i].msg_len, msgvec[i].msg_len);
        }
    }

    return result;
}
#endif

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
        case AERON_FRAME_IN_DROPPED:
            return "FRAME_IN_DROPPED";
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

static const char *dissect_command_type_id(int64_t cmd_type_id)
{
    switch (cmd_type_id)
    {
        case AERON_COMMAND_ADD_PUBLICATION:
            return "ADD_PUBLICATION";

        case AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION:
            return "ADD_EXCLUSIVE_PUBLICATION";

        case AERON_COMMAND_REMOVE_PUBLICATION:
            return "REMOVE_PUBLICATION";

        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
            return "REMOVE_SUBSCRIPTION";

        case AERON_COMMAND_REMOVE_COUNTER:
            return "REMOVE_COUNTER";

        case AERON_COMMAND_ADD_DESTINATION:
            return "ADD_DESTINATION";

        case AERON_COMMAND_REMOVE_DESTINATION:
            return "REMOVE_DESTINATION";

        case AERON_COMMAND_TERMINATE_DRIVER:
            return "TERMINATE_DRIVER";

        default:
            return "unknown command";
    }
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
                dissect_command_type_id(cmd_id),
                command->stream_id,
                command->channel_length,
                channel,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_REMOVE_PUBLICATION:
        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
        case AERON_COMMAND_REMOVE_COUNTER:
        {
            aeron_remove_command_t *command = (aeron_remove_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s %" PRId64 " [%" PRId64 ":%" PRId64 "]",
                dissect_command_type_id(cmd_id),
                command->registration_id,
                command->correlated.client_id,
                command->correlated.correlation_id);

            break;
        }

        case AERON_COMMAND_ADD_SUBSCRIPTION:
        {
            aeron_subscription_command_t *command = (aeron_subscription_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_subscription_command_t);
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

        case AERON_COMMAND_ADD_DESTINATION:
        case AERON_COMMAND_REMOVE_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_destination_command_t);
            snprintf(buffer, sizeof(buffer) - 1, "%s %*s %" PRId64 " [%" PRId64 ":%" PRId64 "]",
                dissect_command_type_id(cmd_id),
                command->channel_length,
                channel,
                command->registration_id,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_ADD_COUNTER:
        {
            aeron_counter_command_t *command = (aeron_counter_command_t *)message;

            const uint8_t *cursor = (const uint8_t *)message + sizeof(aeron_counter_command_t);
            int32_t key_length = *((int32_t *)cursor);
            const uint8_t *key = cursor + sizeof(int32_t);

            cursor = key + key_length;
            int32_t label_length = *((int32_t *)cursor);

            snprintf(buffer, sizeof(buffer) - 1, "ADD_COUNTER %d [%d %d][%d %d][%" PRId64 ":%" PRId64 "]",
                command->type_id,
                (int)(sizeof(aeron_counter_command_t) + sizeof(int32_t)),
                key_length,
                (int)(sizeof(aeron_counter_command_t) + (2 * sizeof(int32_t)) + AERON_ALIGN(key_length, sizeof(int32_t))),
                label_length,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_CLIENT_CLOSE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "CLIENT_CLOSE [%" PRId64 ":%" PRId64 "]",
                command->client_id,
                command->correlation_id);
            break;
        }

        case AERON_COMMAND_TERMINATE_DRIVER:
        {
            aeron_terminate_driver_command_t *command = (aeron_terminate_driver_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s %" PRId64 " %d",
                dissect_command_type_id(cmd_id),
                command->correlated.client_id,
                command->token_length);
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
            aeron_operation_succeeded_t *command = (aeron_operation_succeeded_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "ON_OPERATION_SUCCEEDED %" PRId64, command->correlation_id);
            break;
        }

        case AERON_RESPONSE_ON_PUBLICATION_READY:
        case AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY:
        {
            aeron_publication_buffers_ready_t *command = (aeron_publication_buffers_ready_t *)message;

            const char *log_file_name = (const char *)message + sizeof(aeron_publication_buffers_ready_t);
            snprintf(buffer, sizeof(buffer) - 1, "%s %d:%d %d %d [%" PRId64 " %" PRId64 "]\n    \"%*s\"",
                cmd_id == AERON_RESPONSE_ON_PUBLICATION_READY ? "ON_PUBLICATION_READY" : "ON_EXCLUSIVE_PUBLICATION_READY",
                command->session_id,
                command->stream_id,
                command->position_limit_counter_id,
                command->channel_status_indicator_id,
                command->correlation_id,
                command->registration_id,
                command->log_file_length,
                log_file_name);
            break;
        }

        case AERON_RESPONSE_ON_SUBSCRIPTION_READY:
        {
            aeron_subscription_ready_t *command = (aeron_subscription_ready_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "ON_SUBSCRIPTION_READY %" PRId64 " %d",
                command->correlation_id,
                command->channel_status_indicator_id);
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

            len = snprintf(buffer, sizeof(buffer) - 1, "ON_AVAILABLE_IMAGE %d:%d [%" PRId32 ":%" PRId64 "]",
                command->session_id,
                command->stream_id,
                command->subscriber_position_id,
                command->subscriber_registration_id);

            char *log_file_name_ptr = (char *)message + sizeof(aeron_image_buffers_ready_t);
            int32_t *log_file_name_length = (int32_t *)log_file_name_ptr;
            const char *log_file_name = log_file_name_ptr + sizeof(int32_t);

            char *source_identity_ptr =
                log_file_name_ptr + AERON_ALIGN(*log_file_name_length, sizeof(int32_t)) + sizeof(int32_t);
            int32_t *source_identity_length = (int32_t *)source_identity_ptr;
            const char *source_identity = source_identity_ptr + sizeof(int32_t);
            len += snprintf(ptr + len, sizeof(buffer) - 1 - len, " \"%*s\" [%" PRId64 "]\n",
                *source_identity_length, source_identity, command->correlation_id);

            len += snprintf(ptr + len, sizeof(buffer) - 1 - len, "    \"%*s\"", *log_file_name_length, log_file_name);
            break;
        }

        case AERON_RESPONSE_ON_COUNTER_READY:
        {
            aeron_counter_update_t *command = (aeron_counter_update_t *)message;

            snprintf(buffer, sizeof(buffer) -1 , "ON_COUNTER_READY %" PRId64 " %d",
                command->correlation_id,
                command->counter_id);
            break;
        }

        case AERON_RESPONSE_ON_CLIENT_TIMEOUT:
        {
            aeron_client_timeout_t *command = (aeron_client_timeout_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "ON_CLIENT_TIMEOUT %" PRId64, command->client_id);
            break;
        }

        default:
            break;
    }

    return buffer;
}

static const char *dissect_sockaddr(const struct sockaddr *addr, size_t sockaddr_len)
{
    static char addr_buffer[128], buffer[256];
    unsigned short port = 0;

    if (AF_INET == addr->sa_family)
    {
        struct sockaddr_in *addr4 = (struct sockaddr_in *)addr;

        inet_ntop(AF_INET, &addr4->sin_addr, addr_buffer, sizeof(addr_buffer));
        port = ntohs(addr4->sin_port);
    }
    else if (AF_INET6 == addr->sa_family)
    {
        struct sockaddr_in6 *addr6 = (struct sockaddr_in6 *)addr;

        inet_ntop(AF_INET6, &addr6->sin6_addr, addr_buffer, sizeof(addr_buffer));
        port = ntohs(addr6->sin6_port);
    }
    else
    {
        snprintf(addr_buffer, sizeof(addr_buffer) - 1, "%s", "unknown");
    }

    snprintf(buffer, sizeof(buffer) - 1, "%s.%d", addr_buffer, port);

    return buffer;
}

static const char *dissect_frame(const void *message, size_t length)
{
    static char buffer[256];
    aeron_frame_header_t *hdr = (aeron_frame_header_t *)message;

    buffer[0] = '\0';
    switch (hdr->type)
    {
        case AERON_HDR_TYPE_DATA:
        case AERON_HDR_TYPE_PAD:
        {
            aeron_data_header_t *data = (aeron_data_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s 0x%x len %d %d:%d:%d @%x",
                (hdr->type == AERON_HDR_TYPE_DATA) ? "DATA" : "PAD",
                hdr->flags,
                hdr->frame_length,
                data->session_id,
                data->stream_id,
                data->term_id,
                data->term_offset);
            break;
        }

        case AERON_HDR_TYPE_SM:
        {
            aeron_status_message_header_t *sm = (aeron_status_message_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "SM 0x%x len %d %d:%d:%d @%x %d %" PRId64,
                hdr->flags,
                hdr->frame_length,
                sm->session_id,
                sm->stream_id,
                sm->consumption_term_id,
                sm->consumption_term_offset,
                sm->receiver_window,
                sm->receiver_id);
            break;
        }

        case AERON_HDR_TYPE_NAK:
        {
            aeron_nak_header_t *nak = (aeron_nak_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "NAK 0x%x len %d %d:%d:%d @%x %d",
                hdr->flags,
                hdr->frame_length,
                nak->session_id,
                nak->stream_id,
                nak->term_id,
                nak->term_offset,
                nak->length);
            break;
        }

        case AERON_HDR_TYPE_SETUP:
        {
            aeron_setup_header_t *setup = (aeron_setup_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "SETUP 0x%x len %d %d:%d:%d %d @%x %d MTU %d TTL %d",
                hdr->flags,
                hdr->frame_length,
                setup->session_id,
                setup->stream_id,
                setup->active_term_id,
                setup->initial_term_id,
                setup->term_offset,
                setup->term_length,
                setup->mtu,
                setup->ttl);
            break;
        }

        case AERON_HDR_TYPE_RTTM:
        {
            aeron_rttm_header_t *rttm = (aeron_rttm_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "RTT 0x%x len %d %d:%d %" PRId64 " %" PRId64 " %" PRId64,
                hdr->flags,
                hdr->frame_length,
                rttm->session_id,
                rttm->stream_id,
                rttm->echo_timestamp,
                rttm->reception_delta,
                rttm->receiver_id);
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
                dissect_cmd_out(
                    hdr->cmd_id,
                    (const char *)message + sizeof(aeron_driver_agent_cmd_log_header_t),
                    length - sizeof(aeron_driver_agent_cmd_log_header_t)));
            break;
        }

        case AERON_CMD_IN:
        {
            aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)message;

            printf(
                "[%s] %s %s\n",
                dissect_timestamp(hdr->time_ms),
                dissect_msg_type_id(msg_type_id),
                dissect_cmd_in(
                    hdr->cmd_id,
                    (const char *)message + sizeof(aeron_driver_agent_cmd_log_header_t),
                    length - sizeof(aeron_driver_agent_cmd_log_header_t)));
            break;
        }

        case AERON_FRAME_IN:
        case AERON_FRAME_IN_DROPPED:
        case AERON_FRAME_OUT:
        {
            aeron_driver_agent_frame_log_header_t *hdr = (aeron_driver_agent_frame_log_header_t *)message;
            const struct sockaddr *addr =
                (const struct sockaddr *)((const char *)message + sizeof(aeron_driver_agent_frame_log_header_t));
            const char *frame =
                (const char *)message + sizeof(aeron_driver_agent_frame_log_header_t) + hdr->sockaddr_len;

            printf(
                "[%s] [%d:%d] %s %s: %s\n",
                dissect_timestamp(hdr->time_ms),
                hdr->result,
                (int)hdr->message_len,
                dissect_msg_type_id(msg_type_id),
                dissect_sockaddr(addr, (size_t)hdr->sockaddr_len),
                dissect_frame(frame, (size_t)hdr->message_len));
            break;
        }

        case AERON_MAP_RAW_LOG_OP:
        {
            aeron_driver_agent_map_raw_log_op_header_t *hdr = (aeron_driver_agent_map_raw_log_op_header_t *)message;
            const char *pathname = (const char *)message + sizeof(aeron_driver_agent_map_raw_log_op_header_t);

            printf(
                "[%s] MAP_RAW_LOG %p, \"%*s\" = %d\n",
                dissect_timestamp(hdr->time_ms),
                (void *)hdr->map_raw_log.addr,
                hdr->map_raw_log.path_len,
                pathname,
                hdr->map_raw_log.result);
            break;
        }

        case AERON_MAP_RAW_LOG_OP_CLOSE:
        {
            aeron_driver_agent_map_raw_log_op_header_t *hdr = (aeron_driver_agent_map_raw_log_op_header_t *)message;

            printf(
                "[%s] MAP_RAW_LOG_CLOSE %p = %d\n",
                dissect_timestamp(hdr->time_ms),
                (void *)hdr->map_raw_log_close.addr,
                hdr->map_raw_log_close.result);
            break;
        }

        default:
            break;
    }
}

