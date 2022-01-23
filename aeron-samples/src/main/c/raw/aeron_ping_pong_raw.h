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

#ifndef AERON_AERON_PING_PONG_RAW_H
#define AERON_AERON_PING_PONG_RAW_H

#include <stdlib.h>
#include <stdbool.h>
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

#define AERON_RAW_DEFAULT_PING_HOST "127.0.0.1"
#define AERON_RAW_DEFAULT_PING_PORT (13334)
#define AERON_RAW_DEFAULT_PONG_HOST "127.0.0.1"
#define AERON_RAW_DEFAULT_PONG_PORT (13335)

const char usage_str[] =
    "[-h][-v][-h host][-p port][-H host][-P port][-m messages][-w messages]\n"
    "    -?               help\n"
    "    -h host          ping host\n"
    "    -p port          ping port\n"
    "    -H host          pong host\n"
    "    -P port          pong port\n"
    "    -m messages      number of messages\n"
    "    -w messages      number of warm up messages to send\n";

struct aeron_ping_pong_config_stct
{
    struct sockaddr_storage ping_host;
    struct sockaddr_storage pong_host;
    long messages;
    long warmup_messages;
    bool show_help;
    bool is_server;
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

    while ((opt = getopt(argc, argv, "?h:p:H:P:m:w:s")) != -1)
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

            case 's':
                config->is_server = true;
                break;
        }
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


#endif //AERON_AERON_PING_PONG_RAW_H
