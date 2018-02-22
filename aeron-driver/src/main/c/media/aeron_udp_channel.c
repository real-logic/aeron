/*
 * Copyright 2014-2018 Real Logic Ltd.
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
#include <stdio.h>
#include "aeron_alloc.h"
#include "util/aeron_strutil.h"
#include "uri/aeron_uri.h"
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "media/aeron_udp_channel.h"

int aeron_ipv4_multicast_control_address(struct sockaddr_in *data_addr, struct sockaddr_in *control_addr)
{
    uint8_t bytes[sizeof(struct in_addr)];
    size_t addr_len = sizeof(struct in_addr);
    size_t last_byte_index = addr_len - 1;

    memcpy(bytes, &(data_addr->sin_addr), addr_len);

    if ((bytes[last_byte_index] & 0x1) == 0)
    {
        aeron_set_err(EINVAL, "%s", "Multicast data address must be odd");
        return -1;
    }

    bytes[last_byte_index]++;
    control_addr->sin_family = data_addr->sin_family;
    memcpy(&(control_addr->sin_addr), bytes, addr_len);
    control_addr->sin_port = data_addr->sin_port;

    return 0;
}

int aeron_ipv6_multicast_control_address(struct sockaddr_in6 *data_addr, struct sockaddr_in6 *control_addr)
{
    uint8_t bytes[sizeof(struct in6_addr)];
    size_t addr_len = sizeof(struct in6_addr);
    size_t last_byte_index = addr_len - 1;

    memcpy(bytes, &(data_addr->sin6_addr), addr_len);

    if ((bytes[last_byte_index] & 0x1) == 0)
    {
        aeron_set_err(EINVAL, "%s", "Multicast data address must be odd");
        return -1;
    }

    bytes[last_byte_index]++;
    control_addr->sin6_family = data_addr->sin6_family;
    memcpy(&(control_addr->sin6_addr), bytes, addr_len);
    control_addr->sin6_port = data_addr->sin6_port;

    return 0;
}

int aeron_multicast_control_address(struct sockaddr_storage *data_addr, struct sockaddr_storage *control_addr)
{
    if (AF_INET6 == data_addr->ss_family)
    {
        return aeron_ipv6_multicast_control_address((struct sockaddr_in6 *)data_addr, (struct sockaddr_in6 *)control_addr);
    }
    else if (AF_INET == data_addr->ss_family)
    {
        return aeron_ipv4_multicast_control_address((struct sockaddr_in *)data_addr, (struct sockaddr_in *)control_addr);
    }

    aeron_set_err(EINVAL, "unknown address family: %d", data_addr->ss_family);
    return -1;
}

int aeron_find_multicast_interface(
    int family, const char *interface_str, struct sockaddr_storage *interface_addr, unsigned int *interface_index)
{
    char *wildcard_str = (AF_INET6 == family) ? "[0::]/0" : "0.0.0.0/0";

    return aeron_find_interface((NULL == interface_str) ? wildcard_str : interface_str, interface_addr, interface_index);
}

int aeron_find_unicast_interface(
    int family, const char *interface_str, struct sockaddr_storage *interface_addr, unsigned int *interface_index)
{
    *interface_index = 0;

    if (NULL != interface_str)
    {
        struct sockaddr_storage tmp_addr;
        size_t prefixlen = 0;

        if (aeron_interface_parse_and_resolve(interface_str, &tmp_addr, &prefixlen) >= 0 &&
            aeron_is_wildcard_addr(&tmp_addr))
        {
            memcpy(interface_addr, &tmp_addr, sizeof(tmp_addr));
            return 0;
        }

        return aeron_find_interface(interface_str, interface_addr, interface_index);
    }
    else if (AF_INET6 == family)
    {
        interface_addr->ss_family = AF_INET6;
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)interface_addr;
        addr->sin6_addr = in6addr_any;
        addr->sin6_port = htons(0);
    }
    else
    {
        interface_addr->ss_family = AF_INET;
        struct sockaddr_in *addr = (struct sockaddr_in *)interface_addr;
        addr->sin_addr.s_addr = INADDR_ANY;
        addr->sin_port = htons(0);
    }

    return 0;
}

int aeron_uri_udp_canonicalize(
    char *canonical_form, size_t length, struct sockaddr_storage *local_data, struct sockaddr_storage *remote_data)
{
    char local_data_str[AERON_FORMAT_HEX_LENGTH(sizeof(struct sockaddr_in6))];
    char remote_data_str[AERON_FORMAT_HEX_LENGTH(sizeof(struct sockaddr_in6))];
    uint8_t *local_data_addr, *remote_data_addr;
    unsigned short local_data_port = 0, remote_data_port = 0;
    size_t local_data_length = sizeof(struct in_addr), remote_data_length = sizeof(struct in_addr);

    if (AF_INET6 == local_data->ss_family)
    {
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)local_data;

        local_data_addr = (uint8_t *)&addr->sin6_addr;
        local_data_port = ntohs(addr->sin6_port);
        local_data_length = sizeof(struct in6_addr);
    }
    else
    {
        struct sockaddr_in *addr = (struct sockaddr_in *)local_data;

        local_data_addr = (uint8_t *)&addr->sin_addr;
        local_data_port = ntohs(addr->sin_port);
    }

    if (AF_INET6 == remote_data->ss_family)
    {
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)remote_data;

        remote_data_addr = (uint8_t *)&addr->sin6_addr;
        remote_data_port = ntohs(addr->sin6_port);
        remote_data_length = sizeof(struct in6_addr);
    }
    else
    {
        struct sockaddr_in *addr = (struct sockaddr_in *)remote_data;

        remote_data_addr = (uint8_t *)&addr->sin_addr;
        remote_data_port = ntohs(addr->sin_port);
    }

    aeron_format_to_hex(local_data_str, sizeof(local_data_str), local_data_addr, local_data_length);
    aeron_format_to_hex(remote_data_str, sizeof(remote_data_str), remote_data_addr, remote_data_length);

    return snprintf(
        canonical_form, length, "UDP-%s-%d-%s-%d", local_data_str, local_data_port, remote_data_str, remote_data_port);
}

int aeron_udp_channel_parse(const char *uri, size_t uri_length, aeron_udp_channel_t **channel)
{
    aeron_udp_channel_t *_channel = NULL;
    struct sockaddr_storage endpoint_addr, explicit_control_addr, interface_addr;
    unsigned int interface_index = 0;

    memset(&endpoint_addr, 0, sizeof(endpoint_addr));
    memset(&explicit_control_addr, 0, sizeof(explicit_control_addr));
    memset(&interface_addr, 0, sizeof(interface_addr));

    if (aeron_alloc((void **)&_channel, sizeof(aeron_udp_channel_t)) < 0)
    {
        aeron_set_err(ENOMEM, "%s", "could not allocate UDP channel");
        return -1;
    }

    if (aeron_uri_parse(uri, &_channel->uri) < 0)
    {
        goto error_cleanup;
    }

    strncpy(_channel->original_uri, uri, sizeof(_channel->original_uri));
    _channel->uri_length = uri_length;
    _channel->explicit_control = false;
    _channel->multicast = false;

    if (_channel->uri.type != AERON_URI_UDP)
    {
        aeron_set_err(EINVAL, "%s", "UDP channels must use UDP URIs");
        goto error_cleanup;
    }

    if (NULL == _channel->uri.params.udp.endpoint_key && NULL == _channel->uri.params.udp.control_key)
    {
        aeron_set_err(EINVAL, "%s", "Aeron URIs for UDP must specify an endpoint address and/or a control address");
        goto error_cleanup;
    }

    if (NULL != _channel->uri.params.udp.endpoint_key)
    {
        if (aeron_host_and_port_parse_and_resolve(_channel->uri.params.udp.endpoint_key, &endpoint_addr) < 0)
        {
            aeron_set_err(
                aeron_errcode(), "could not resolve endpoint address=(%s): %s", _channel->uri.params.udp.endpoint_key, aeron_errmsg());
            goto error_cleanup;
        }
    }

    if (NULL != _channel->uri.params.udp.control_key)
    {
        if (aeron_host_and_port_parse_and_resolve(_channel->uri.params.udp.control_key, &explicit_control_addr) < 0)
        {
            aeron_set_err(
                aeron_errcode(), "could not resolve control address=(%s): %s", _channel->uri.params.udp.control_key, aeron_errmsg());
            goto error_cleanup;
        }
    }

    if (aeron_is_addr_multicast(&endpoint_addr))
    {
        memcpy(&_channel->remote_data, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        if (aeron_multicast_control_address(&endpoint_addr, &_channel->remote_control) < 0)
        {
            goto error_cleanup;
        }

        if (aeron_find_multicast_interface(
            endpoint_addr.ss_family, _channel->uri.params.udp.interface_key, &interface_addr, &interface_index) < 0)
        {
            aeron_set_err(
                aeron_errcode(), "could not find interface=(%s): %s", _channel->uri.params.udp.interface_key, aeron_errmsg());
            goto error_cleanup;
        }

        _channel->interface_index = interface_index;
        _channel->multicast_ttl = aeron_uri_multicast_ttl(&_channel->uri);
        memcpy(&_channel->local_data, &interface_addr, AERON_ADDR_LEN(&interface_addr));
        memcpy(&_channel->local_control, &interface_addr, AERON_ADDR_LEN(&interface_addr));
        aeron_uri_udp_canonicalize(
            _channel->canonical_form, sizeof(_channel->canonical_form), &interface_addr, &endpoint_addr);
        _channel->canonical_length = strlen(_channel->canonical_form);
        _channel->multicast = true;
    }
    else if (NULL != _channel->uri.params.udp.control_key)
    {
        _channel->interface_index = 0;
        _channel->multicast_ttl = 0;
        memcpy(&_channel->remote_data, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        memcpy(&_channel->remote_control, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        memcpy(&_channel->local_data, &explicit_control_addr, AERON_ADDR_LEN(&explicit_control_addr));
        memcpy(&_channel->local_control, &explicit_control_addr, AERON_ADDR_LEN(&explicit_control_addr));
        aeron_uri_udp_canonicalize(
            _channel->canonical_form, sizeof(_channel->canonical_form), &explicit_control_addr, &endpoint_addr);
        _channel->canonical_length = strlen(_channel->canonical_form);
        _channel->explicit_control = true;
    }
    else
    {
        if (aeron_find_unicast_interface(
            endpoint_addr.ss_family, _channel->uri.params.udp.interface_key, &interface_addr, &interface_index) < 0)
        {
            goto error_cleanup;
        }

        _channel->interface_index = interface_index;
        _channel->multicast_ttl = 0;
        memcpy(&_channel->remote_data, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        memcpy(&_channel->remote_control, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        memcpy(&_channel->local_data, &interface_addr, AERON_ADDR_LEN(&interface_addr));
        memcpy(&_channel->local_control, &interface_addr, AERON_ADDR_LEN(&interface_addr));
        aeron_uri_udp_canonicalize(
            _channel->canonical_form, sizeof(_channel->canonical_form), &interface_addr, &endpoint_addr);
        _channel->canonical_length = strlen(_channel->canonical_form);
    }

    *channel = _channel;
    return 0;

    error_cleanup:

        *channel = NULL;
        if (NULL != _channel)
        {
            aeron_udp_channel_delete(_channel);
        }

        return -1;
}

void aeron_udp_channel_delete(aeron_udp_channel_t *channel)
{
    if (NULL != channel)
    {
        aeron_uri_close(&channel->uri);
        aeron_free(channel);
    }
}
