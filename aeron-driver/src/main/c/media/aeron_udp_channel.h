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

#ifndef AERON_UDP_CHANNEL_H
#define AERON_UDP_CHANNEL_H

#include "aeron_socket.h"
#include "uri/aeron_uri.h"

typedef struct aeron_udp_channel_stct
{
    char original_uri[AERON_MAX_PATH];
    char canonical_form[AERON_MAX_PATH];
    aeron_uri_t uri;
    struct sockaddr_storage remote_data;
    struct sockaddr_storage local_data;
    struct sockaddr_storage remote_control;
    struct sockaddr_storage local_control;
    unsigned int interface_index;
    size_t uri_length;
    size_t canonical_length;
    uint8_t multicast_ttl;
    bool explicit_control;
    bool multicast;
}
aeron_udp_channel_t;

int aeron_udp_channel_parse(size_t uri_length, const char *uri, aeron_udp_channel_t **channel);

void aeron_udp_channel_delete(aeron_udp_channel_t *channel);

#endif //AERON_UDP_CHANNEL_H
