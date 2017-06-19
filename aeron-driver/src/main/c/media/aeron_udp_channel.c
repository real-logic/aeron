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

#include <string.h>
#include <errno.h>
#include <netdb.h>
#include "uri/aeron_uri.h"
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "media/aeron_udp_channel.h"

int aeron_ipv4_multicast_control_address(struct sockaddr_in *data_addr, struct sockaddr_in *control_addr)
{
    uint8_t bytes[sizeof(struct in_addr)];
    size_t last_byte_index = 0, addr_len = 0;

    addr_len = sizeof(struct in_addr);
    memcpy(bytes, &(data_addr->sin_addr), addr_len);
    last_byte_index = addr_len - 1;

    if ((bytes[last_byte_index] & 0x1) == 0)
    {
        aeron_set_err(EINVAL, "%s", "Multicast data address must be odd");
        return -1;
    }

    bytes[last_byte_index]++;
    control_addr->sin_family = data_addr->sin_family;
    memcpy(&(control_addr->sin_addr), bytes, addr_len);

    return 0;
}

int aeron_ipv6_multicast_control_address(struct sockaddr_in6 *data_addr, struct sockaddr_in6 *control_addr)
{
    uint8_t bytes[sizeof(struct in6_addr)];
    size_t last_byte_index = 0, addr_len = 0;

    addr_len = sizeof(struct in_addr);
    memcpy(bytes, &(data_addr->sin6_addr), addr_len);
    last_byte_index = addr_len - 1;

    if ((bytes[last_byte_index] & 0x1) == 0)
    {
        aeron_set_err(EINVAL, "%s", "Multicast data address must be odd");
        return -1;
    }

    bytes[last_byte_index]++;
    control_addr->sin6_family = data_addr->sin6_family;
    memcpy(&(control_addr->sin6_addr), bytes, addr_len);

    return 0;
}

int aeron_multicast_control_address(struct sockaddr *data_addr, struct sockaddr *control_addr)
{
    if (AF_INET6 == data_addr->sa_family)
    {
        return aeron_ipv6_multicast_control_address((struct sockaddr_in6 *)data_addr, (struct sockaddr_in6 *)control_addr);
    }
    else if (AF_INET == data_addr->sa_family)
    {
        return aeron_ipv4_multicast_control_address((struct sockaddr_in *)data_addr, (struct sockaddr_in *)control_addr);
    }

    aeron_set_err(EINVAL, "unknown address family: %d", data_addr->sa_family);
    return -1;
}

int aeron_udp_channel_parse(const char *uri, size_t uri_length, aeron_udp_channel_t *channel)
{
    int result = -1;
    struct sockaddr_storage endpoint_addr, explicit_control_addr;

    memset(&endpoint_addr, 0, sizeof(endpoint_addr));
    memset(&explicit_control_addr, 0, sizeof(explicit_control_addr));

    if ((result = aeron_uri_parse(uri, &channel->uri)) < 0)
    {
        return result;
    }

    strncpy(channel->original_uri, uri, sizeof(channel->original_uri));
    channel->uri_length = uri_length;

    if (channel->uri.type != AERON_URI_UDP)
    {
        aeron_set_err(EINVAL, "%s", "UDP channels must use UDP URIs");
        return -1;
    }

    if (NULL == channel->uri.params.udp.endpoint_key && NULL == channel->uri.params.udp.control_key)
    {
        aeron_set_err(EINVAL, "%s", "Aeron URIs for UDP must specify an endpoint address and/or a control address");
        return -1;
    }

    if (NULL != channel->uri.params.udp.endpoint_key)
    {
        if (aeron_host_and_port_parse_and_resolve(channel->uri.params.udp.endpoint_key, &endpoint_addr) < 0)
        {
            aeron_set_err(
                aeron_errcode(), "could not resolve endpoint address=(%s): %s", channel->uri.params.udp.endpoint_key, aeron_errmsg());
            return -1;
        }
    }

    if (NULL != channel->uri.params.udp.control_key)
    {
        if (aeron_host_and_port_parse_and_resolve(channel->uri.params.udp.control_key, &explicit_control_addr) < 0)
        {
            aeron_set_err(
                aeron_errcode(), "could not resolve control address=(%s): %s", channel->uri.params.udp.control_key, aeron_errmsg());
            return -1;
        }
    }

    if (aeron_is_addr_multicast((struct sockaddr *)&endpoint_addr))
    {

    }
    else if (NULL != channel->uri.params.udp.control_key)
    {


    }
    else
    {

    }


    return -1;
}
