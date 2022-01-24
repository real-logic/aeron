/*
 * Copyright 2014-2022 Real Logic Limited.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>
#include <inttypes.h>
#include <time.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <net/if.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>

#include <hdr_histogram.h>

#define AERON_RAW_DEFAULT_PING_HOST "127.0.0.1"
#define AERON_RAW_DEFAULT_PING_PORT (13334)
#define AERON_RAW_DEFAULT_PONG_HOST "127.0.0.1"
#define AERON_RAW_DEFAULT_PONG_PORT (13335)
#define AERON_RAW_DEFAULT_TRANSPORT_TYPE (1)

const char usage_str[] =
    "[-h][-v][-h host][-p port][-H host][-P port][-m messages][-w messages]\n"
    "    -?               help\n"
    "    -s               run in echo server mode\n"
    "    -t               transport type (1=connect/sendto, 2=sendto, 3=sendmsg)\n"
    "    -h host          ping host (default 127.0.0.1)\n"
    "    -p port          ping port (default 13334)\n"
    "    -H host          pong host (default 127.0.0.1)\n"
    "    -P port          pong port (default 13335)\n"
    "    -m messages      number of messages (default 0)\n"
    "    -w messages      number of warm up messages to send (default 0)\n";

typedef int (*aeron_ping_pong_raw_connect)(
    int send_fd,
    const struct sockaddr *addr,
    socklen_t addr_len);

typedef int (*aeron_ping_pong_raw_send)(
    int send_fd,
    const struct sockaddr *addr,
    socklen_t addr_len,
    void *buffer,
    size_t buffer_len);

int aeron_ping_pong_raw_sendto_connected(
    int send_fd,
    const struct sockaddr *addr,
    socklen_t addr_len,
    void *buffer,
    size_t buffer_len)
{
    int sendto_result = sendto(send_fd, buffer, buffer_len, 0, NULL, 0);
    if (sendto_result < 0)
    {
        fprintf(
            stderr,
            "failed sendto(send_fd, msghdr.msg_iov->iov_base, recvmsg_result, 0, NULL, 0), %s\n",
            strerror(errno));
        return -1;
    }

    return sendto_result;
}

int aeron_ping_pong_raw_sendto_unconnected(
    int send_fd,
    const struct sockaddr *addr,
    socklen_t addr_len,
    void *buffer,
    size_t buffer_len)
{
    int sendto_result = sendto(send_fd, buffer, buffer_len, 0, addr, addr_len);
    if (sendto_result < 0)
    {
        fprintf(
            stderr,
            "failed sendto(send_fd, msghdr.msg_iov->iov_base, recvmsg_result, 0, addr, addr_len), %s\n",
            strerror(errno));
        return -1;
    }

    return sendto_result;
}

int aeron_ping_pong_raw_sendmsg(
    int send_fd,
    const struct sockaddr *addr,
    socklen_t addr_len,
    void *buffer,
    size_t buffer_len)
{
    struct msghdr send_msghdr = { 0 };
    struct iovec send_iov = { 0 };
    send_msghdr.msg_iov = &send_iov;
    send_iov.iov_base = buffer;
    send_iov.iov_len = buffer_len;
    send_msghdr.msg_control = NULL;
    send_msghdr.msg_controllen = 0;
    send_msghdr.msg_name = (void *)addr;
    send_msghdr.msg_namelen = addr_len;
    send_msghdr.msg_iovlen = 1;

    int sendmsg_result = sendmsg(send_fd, &send_msghdr, 0);
    if (sendmsg_result < 0)
    {
        fprintf(stderr, "failed sendmsg(send_fd, &send_msghdr, 0), %s\n", strerror(errno));
        return -1;
    }

    return sendmsg_result;
}

int aeron_ping_pong_raw_socket_connect(
    int send_fd,
    const struct sockaddr *addr,
    socklen_t addr_len)
{
    int connect_result = connect(send_fd, addr, addr_len);
    if (connect_result < 0)
    {
        fprintf(stderr, "failed send connect(send_fd, send_addr, sizeof(struct sockaddr_in)), %s\n", strerror(errno));
        return -1;
    }

    return connect_result;
}

int aeron_ping_pong_raw_socket_null_connect(
    int send_fd,
    const struct sockaddr *addr,
    socklen_t addr_len)
{
    return 0;
}

struct aeron_ping_pong_config_stct
{
    struct sockaddr_storage ping_host;
    struct sockaddr_storage pong_host;
    long messages;
    long warmup_messages;
    bool show_help;
    bool is_server;
    int transport_type;
    aeron_ping_pong_raw_connect connect_func;
    aeron_ping_pong_raw_send send_func;
};
typedef struct aeron_ping_pong_config_stct aeron_ping_pong_config_t;

int aeron_ping_pong_parse_config(int argc, char **argv, aeron_ping_pong_config_t *config)
{
    int opt;
    struct sockaddr_in *ping_addr = (struct sockaddr_in *)&config->ping_host;
    struct sockaddr_in *pong_addr = (struct sockaddr_in *)&config->pong_host;

    ping_addr->sin_family = AF_INET;
    inet_pton(AF_INET, AERON_RAW_DEFAULT_PING_HOST, &ping_addr->sin_addr);
    ping_addr->sin_port = htons(AERON_RAW_DEFAULT_PING_PORT);
    pong_addr->sin_family = AF_INET;
    inet_pton(AF_INET, AERON_RAW_DEFAULT_PONG_HOST, &pong_addr->sin_addr);
    pong_addr->sin_port = htons(AERON_RAW_DEFAULT_PONG_PORT);
    config->transport_type = AERON_RAW_DEFAULT_TRANSPORT_TYPE;

    while ((opt = getopt(argc, argv, "?h:p:H:P:m:w:t:s")) != -1)
    {
        switch (opt)
        {
            case '?':
                config->show_help = true;
                break;

            case 'h':
                if (-1 == inet_pton(AF_INET, optarg, &ping_addr->sin_addr))
                {
                    return -1;
                }
                break;

            case 'H':
                if (-1 == inet_pton(AF_INET, optarg, &pong_addr->sin_addr))
                {
                    return -1;
                }
                break;

            case 'm':
                // TODO: error handling
                config->messages = strtol(optarg, NULL, 10);
                break;

            case 'w':
                // TODO: error handling
                config->warmup_messages = strtol(optarg, NULL, 10);
                break;

            case 't':
            {
                config->transport_type = atoi(optarg);
                break;
            }

            case 's':
                config->is_server = true;
                break;
        }
    }

    switch (config->transport_type)
    {
        case 1:
            config->connect_func = aeron_ping_pong_raw_socket_connect;
            config->send_func = aeron_ping_pong_raw_sendto_connected;
            printf("Transport: connect/sendto\n");
            break;

        case 2:
            config->connect_func = aeron_ping_pong_raw_socket_null_connect;
            config->send_func = aeron_ping_pong_raw_sendto_unconnected;
            printf("Transport: sendto\n");
            break;

        case 3:
            config->connect_func = aeron_ping_pong_raw_socket_null_connect;
            config->send_func = aeron_ping_pong_raw_sendmsg;
            printf("Transport: sendmsg\n");
            break;

        default:
            fprintf(stderr, "Invalid transport type: %d\n", config->transport_type);
            return -1;
    }

    return 0;
}

int aeron_ping_pong_raw_set_socket_non_blocking(int fd)
{
    int flags;
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0)
    {
        return -1;
    }

    flags |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) < 0)
    {
        return -1;
    }

    return 0;
}

int recv_then_send(aeron_ping_pong_config_t *config, int send_fd, int recv_fd)
{
    int8_t buf[64 * 1024];
    struct msghdr msghdr = { 0 };
    struct iovec iov = { 0 };
    iov.iov_base = buf;
    iov.iov_len = sizeof(buf);
    msghdr.msg_iov = &iov;
    msghdr.msg_iovlen = 1;

    while (true)
    {
        ssize_t recvmsg_result = recvmsg(recv_fd, &msghdr, 0);
        if (recvmsg_result < 0 && errno != EAGAIN)
        {
            fprintf(stderr, "failed recvmsg(recv_fd, &msghdr, 0), %s\n", strerror(errno));
            return -1;
        }
        else if (0 < recvmsg_result)
        {
            if (config->send_func(
                send_fd,
                (const struct sockaddr *)&config->pong_host,
                sizeof(struct sockaddr_in),
                msghdr.msg_iov->iov_base,
                recvmsg_result) < 0)
            {
                return -1;
            }
        }
    }
}

int send_then_recv(aeron_ping_pong_config_t *config, int send_fd, int recv_fd, long messages, struct hdr_histogram *h)
{
    int8_t send_buf[32];
    int8_t buf[64 * 1024];
    struct msghdr msghdr = { 0 };
    struct iovec iov = { 0 };
    iov.iov_base = buf;
    iov.iov_len = sizeof(buf);
    msghdr.msg_iov = &iov;
    msghdr.msg_iovlen = 1;
    struct timespec send_ts;
    struct timespec recv_ts;

    for (int i = 0; i < messages; i++)
    {
        clock_gettime(CLOCK_MONOTONIC, &send_ts);
        if (config->send_func(
            send_fd,
            (const struct sockaddr *)&config->ping_host,
            sizeof(struct sockaddr_in),
            send_buf,
            sizeof(send_buf)) < 0)
        {
            return -1;
        }

        do
        {
            ssize_t recvmsg_result = recvmsg(recv_fd, &msghdr, 0);
            if (recvmsg_result < 0 && EAGAIN != errno)
            {
                fprintf(stderr, "failed (send) recvmsg(recv_fd, &msghdr, 0), %s\n", strerror(errno));
                return -1;
            }
            else if (recvmsg_result > 0)
            {
                break;
            }
        }
        while (true);

        clock_gettime(CLOCK_MONOTONIC, &recv_ts);
        int64_t delta_ns = ((recv_ts.tv_sec * INT64_C(1000000000)) + recv_ts.tv_nsec) - ((send_ts.tv_sec * INT64_C(1000000000)) + send_ts.tv_nsec);
        hdr_record_value(h, delta_ns);
    }

    return 0;
}

int main(int argc, char **argv)
{
    aeron_ping_pong_config_t config = { 0 };
    aeron_ping_pong_parse_config(argc, argv, &config);
    struct sockaddr_in *send_addr = config.is_server ? (struct sockaddr_in *)&config.pong_host : (struct sockaddr_in *)&config.ping_host;
    struct sockaddr_in *recv_addr = config.is_server ? (struct sockaddr_in *)&config.ping_host : (struct sockaddr_in *)&config.pong_host;
    struct hdr_histogram *histogram;
    if (hdr_init(1, 1000000000, 3, &histogram) < 0)
    {
        fprintf(stderr, "failed to hdr_init(0, 1000000000, 3, &histogram)\n");
        return -1;
    }

    int send_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (send_fd < 0)
    {
        fprintf(stderr, "failed send socket(AF_INET, SOCK_DGRAM, 0), %s\n", strerror(errno));
        return -1;
    }


    if (config.connect_func(send_fd, (const struct sockaddr *)send_addr, sizeof(*send_addr)))
    {
        return -1;
    }

    if (aeron_ping_pong_raw_set_socket_non_blocking(send_fd) < 0)
    {
        fprintf(stderr, "failed send aeron_ping_pong_raw_set_socket_non_blocking(send_fd), %s\n", strerror(errno));
        return -1;
    }

    int recv_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (recv_fd < 0)
    {
        fprintf(stderr, "failed recv socket(AF_INET, SOCK_DGRAM, 0), %s\n", strerror(errno));
        return -1;
    }

    if (bind(recv_fd, (const struct sockaddr *)recv_addr, sizeof(*recv_addr)) < 0)
    {
        fprintf(stderr, "failed recv bind(recv_fd, recv_addr, sizeof(struct sockaddr_in)), %s\n", strerror(errno));
        return -1;
    }

    if (aeron_ping_pong_raw_set_socket_non_blocking(recv_fd) < 0)
    {
        fprintf(stderr, "failed send aeron_ping_pong_raw_set_socket_non_blocking(recv_fd), %s\n", strerror(errno));
        return -1;
    }

    char send_addr_str[64];
    char recv_addr_str[64];

    inet_ntop(AF_INET, &send_addr->sin_addr, send_addr_str, sizeof(send_addr_str));
    inet_ntop(AF_INET, &recv_addr->sin_addr, recv_addr_str, sizeof(recv_addr_str));

    fprintf(stdout, "send %s:%" PRIu16 "\n", send_addr_str, ntohs(send_addr->sin_port));
    fprintf(stdout, "recv %s:%" PRIu16 "\n", recv_addr_str, ntohs(recv_addr->sin_port));

    if (config.is_server)
    {
        recv_then_send(&config, send_fd, recv_fd);
    }
    else
    {
        printf("sending %ld warmup messages\n", config.warmup_messages);
        if (send_then_recv(&config, send_fd, recv_fd, config.warmup_messages, histogram) < 0)
        {
            return -1;
        }

        hdr_reset(histogram);
        printf("sending %ld real messages\n", config.messages);
        if (send_then_recv(&config, send_fd, recv_fd, config.messages, histogram) < 0)
        {
            return -1;
        }
        hdr_percentiles_print(histogram, stdout, 1, 1000.0, CLASSIC);
    }

    return 0;
}

