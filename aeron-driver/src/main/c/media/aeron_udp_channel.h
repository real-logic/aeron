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

#ifndef AERON_UDP_CHANNEL_H
#define AERON_UDP_CHANNEL_H

#include "aeron_socket.h"
#include "uri/aeron_uri.h"
#include "util/aeron_netutil.h"
#include "aeron_name_resolver.h"

typedef struct aeron_udp_channel_stct
{
    char original_uri[AERON_MAX_PATH];
    char canonical_form[AERON_MAX_PATH];
    aeron_uri_t uri;
    struct sockaddr_storage remote_data;
    struct sockaddr_storage local_data;
    struct sockaddr_storage remote_control;
    struct sockaddr_storage local_control;
    int64_t tag_id;
    unsigned int interface_index;
    size_t uri_length;
    size_t canonical_length;
    uint8_t multicast_ttl;
    bool has_explicit_endpoint;
    bool has_explicit_control;
    bool is_manual_control_mode;
    bool is_dynamic_control_mode;
    bool is_multicast;
    aeron_uri_ats_status_t ats_status;
}
aeron_udp_channel_t;

int aeron_udp_channel_parse(
    size_t uri_length,
    const char *uri,
    aeron_name_resolver_t *resolver,
    aeron_udp_channel_t **channel,
    bool is_destination);

void aeron_udp_channel_delete(aeron_udp_channel_t *channel);

inline bool aeron_udp_channel_is_wildcard(aeron_udp_channel_t *channel)
{
    return aeron_is_wildcard_addr(&channel->remote_data) && aeron_is_wildcard_port(&channel->remote_data) &&
        aeron_is_wildcard_addr(&channel->local_data) && aeron_is_wildcard_port(&channel->local_data);
}

inline bool aeron_udp_channel_equals(aeron_udp_channel_t *a, aeron_udp_channel_t *b)
{
    return a == b || (a != NULL && 0 == strncmp(a->canonical_form, b->canonical_form, AERON_MAX_PATH));
}

#endif //AERON_UDP_CHANNEL_H
