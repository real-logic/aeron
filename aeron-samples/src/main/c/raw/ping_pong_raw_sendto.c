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
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <time.h>
#include <hdr_histogram.h>

#include "aeron_ping_pong_raw.h"

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
            if (sendto(send_fd, msghdr.msg_iov->iov_base, recvmsg_result, 0, NULL, 0) < 0)
            {
                fprintf(stderr, "failed sendto(send_fd, msghdr.msg_iov->iov_base, recvmsg_result, 0, NULL, 0), %s\n", strerror(errno));
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
        if (sendto(send_fd, send_buf, sizeof(send_buf), 0, NULL, 0) < 0)
        {
            fprintf(stderr, "failed to sendto(send_fd, send_buf, sizeof(send_buf), 0, NULL, 0), %s\n", strerror(errno));
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

    if (connect(send_fd, (const struct sockaddr *)send_addr, sizeof(*send_addr)) < 0)
    {
        fprintf(stderr, "failed send connect(send_fd, send_addr, sizeof(struct sockaddr_in)), %s\n", strerror(errno));
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

