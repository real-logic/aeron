/*
 * Copyright 2014-2020 Real Logic Limited.
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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include "aeron_socket.h"
#include <stdio.h>
#include <uri/aeron_uri.h>
#include "aeron_alloc.h"
#include "util/aeron_strutil.h"
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "media/aeron_udp_channel.h"
#include "concurrent/aeron_atomic.h"
#include "command/aeron_control_protocol.h"

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
        return aeron_ipv6_multicast_control_address(
            (struct sockaddr_in6 *)data_addr, (struct sockaddr_in6 *)control_addr);
    }
    else if (AF_INET == data_addr->ss_family)
    {
        return aeron_ipv4_multicast_control_address(
            (struct sockaddr_in *)data_addr, (struct sockaddr_in *)control_addr);
    }

    aeron_set_err(EINVAL, "unknown address family: %d", data_addr->ss_family);
    return -1;
}

int aeron_find_multicast_interface(
    int family, const char *interface_str, struct sockaddr_storage *interface_addr, unsigned int *interface_index)
{
    char *wildcard_str = AF_INET6 == family ? "[0::]/0" : "0.0.0.0/0";

    return aeron_find_interface(NULL == interface_str ? wildcard_str : interface_str, interface_addr, interface_index);
}

#define AERON_URI_ADDRESS_HOST_LEN (INET6_ADDRSTRLEN + 2)
#define AERON_URI_ADDRESS_PORT_LEN (NI_MAXSERV + 1)

/**
 * Internal (assumes sizes of buffers)...  Will prefix the port_buffer with a ':' and wrap IPv6 addresses with '[' and
 * ']'
 *
 * @param addr
 * @param host_buffer assumes at least AERON_URI_ADDRESS_HOST_LEN of space.
 * @param port_buffer assumes at least AERON_URI_ADDRESS_PORT_LEN of space.
 * @return
 */
static int aeron_uri_format_address(struct sockaddr_storage *addr, char *host_buffer, char *port_buffer)
{
    int host_index = addr->ss_family == AF_INET6 ? 1 : 0;

    int result = getnameinfo(
        (const struct sockaddr *)addr, sizeof(struct sockaddr_storage),
        &host_buffer[host_index], INET6_ADDRSTRLEN,
        &port_buffer[1], NI_MAXSERV,
        NI_NUMERICHOST | NI_NUMERICSERV);

    if (0 != result)
    {
        aeron_set_err(EINVAL, "Failed to format ip address: %s", gai_strerror(result));
        return -1;
    }

    port_buffer[0] = ':';

    if (addr->ss_family == AF_INET6)
    {
        host_buffer[0] = '[';
        size_t len = strlen(host_buffer);
        host_buffer[len] = ']';
        host_buffer[len + 1] = '\0';
    }

    return 0;
}

static int32_t unique_canonical_form_value = 0;

int aeron_uri_udp_canonicalise(
    char *canonical_form,
    size_t length,
    const char *local_param_value,
    struct sockaddr_storage *local_data,
    const char *remote_param_value,
    struct sockaddr_storage *remote_data,
    bool make_unique)
{
    char unique_suffix[4 * sizeof(unique_canonical_form_value)] = "";
    char local_data_formatted_addr[AERON_URI_ADDRESS_HOST_LEN];
    char remote_data_formatted_addr[AERON_URI_ADDRESS_HOST_LEN];
    char local_port_str[AERON_URI_ADDRESS_PORT_LEN] = "";
    char remote_port_str[AERON_URI_ADDRESS_PORT_LEN] = "";

    const char *local_data_str;
    const char *remote_data_str;

    if (NULL == local_param_value)
    {
        if (aeron_uri_format_address(local_data, local_data_formatted_addr, local_port_str) < 0)
        {
            return -1;
        }

        local_data_str = local_data_formatted_addr;
    }
    else
    {
        local_data_str = local_param_value;
    }

    if (NULL == remote_param_value)
    {
        if (aeron_uri_format_address(remote_data, remote_data_formatted_addr, remote_port_str) < 0)
        {
            return -1;
        }

        remote_data_str = remote_data_formatted_addr;
    }
    else
    {
        remote_data_str = remote_param_value;
    }

    if (make_unique)
    {
        int32_t result = 0;

        AERON_GET_AND_ADD_INT32(result, unique_canonical_form_value, 1);
        snprintf(unique_suffix, sizeof(unique_suffix) - 1, "-%" PRId32, result);
    }

    return snprintf(
        canonical_form, length, "UDP-%s%s-%s%s%s",
        local_data_str, local_port_str, remote_data_str, remote_port_str, unique_suffix);
}

int aeron_udp_channel_parse(
    size_t uri_length,
    const char *uri,
    aeron_name_resolver_t *resolver,
    aeron_udp_channel_t **channel)
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

    if (aeron_uri_parse(uri_length, uri, &_channel->uri) < 0)
    {
        goto error_cleanup;
    }

    size_t copy_length = sizeof(_channel->original_uri) - 1;
    copy_length = uri_length < copy_length ? uri_length : copy_length;

    memcpy(_channel->original_uri, uri, copy_length);
    _channel->original_uri[copy_length] = '\0';
    _channel->uri_length = copy_length;

    _channel->has_explicit_control = false;
    _channel->is_manual_control_mode = false;
    _channel->is_dynamic_control_mode = false;
    _channel->is_multicast = false;
    _channel->tag_id = AERON_URI_INVALID_TAG;

    if (_channel->uri.type != AERON_URI_UDP)
    {
        aeron_set_err(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s", "UDP channels must use UDP URIs");
        goto error_cleanup;
    }

    if (NULL != _channel->uri.params.udp.control_mode)
    {
        _channel->is_manual_control_mode =
            strcmp(_channel->uri.params.udp.control_mode, AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL_VALUE) == 0;
        _channel->is_dynamic_control_mode =
            strcmp(_channel->uri.params.udp.control_mode, AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC_VALUE) == 0;
    }

    if (_channel->is_dynamic_control_mode && NULL == _channel->uri.params.udp.control)
    {
        aeron_set_err(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s", "explicit control expected with dynamic control mode");
        goto error_cleanup;
    }

    bool has_no_distinguishing_characteristic =
        NULL == _channel->uri.params.udp.endpoint &&
        NULL == _channel->uri.params.udp.control &&
        NULL == _channel->uri.params.udp.channel_tag;

    if (has_no_distinguishing_characteristic && !_channel->is_manual_control_mode)
    {
        aeron_set_err(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s",
            "URIs for UDP must specify endpoint, control, tags, or control-mode=manual");
        goto error_cleanup;
    }

    if (NULL != _channel->uri.params.udp.endpoint)
    {
        if (aeron_name_resolver_resolve_host_and_port(
            resolver, _channel->uri.params.udp.endpoint, AERON_UDP_CHANNEL_ENDPOINT_KEY, false, &endpoint_addr) < 0)
        {
            goto error_cleanup;
        }
    }
    else
    {
        aeron_set_ipv4_wildcard_host_and_port(&endpoint_addr);
    }

    if (NULL != _channel->uri.params.udp.control)
    {
        if (aeron_name_resolver_resolve_host_and_port(
            resolver, _channel->uri.params.udp.control, AERON_UDP_CHANNEL_CONTROL_KEY, false, &explicit_control_addr) < 0)
        {
            goto error_cleanup;
        }
    }

    if (NULL != _channel->uri.params.udp.channel_tag)
    {
        if ((_channel->tag_id = aeron_uri_parse_tag(_channel->uri.params.udp.channel_tag)) == AERON_URI_INVALID_TAG)
        {
            aeron_set_err(-AERON_ERROR_CODE_INVALID_CHANNEL, "could not parse channel tag string: %s",
                _channel->uri.params.udp.channel_tag);
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
                endpoint_addr.ss_family, _channel->uri.params.udp.bind_interface, &interface_addr, &interface_index) < 0)
        {
            aeron_set_err(
                    -AERON_ERROR_CODE_INVALID_CHANNEL,
                    "could not find interface=(%s): %s",
                    _channel->uri.params.udp.bind_interface, aeron_errmsg());
            goto error_cleanup;
        }

        _channel->interface_index = interface_index;
        _channel->multicast_ttl = aeron_uri_multicast_ttl(&_channel->uri);
        memcpy(&_channel->local_data, &interface_addr, AERON_ADDR_LEN(&interface_addr));
        memcpy(&_channel->local_control, &interface_addr, AERON_ADDR_LEN(&interface_addr));
        aeron_uri_udp_canonicalise(
            _channel->canonical_form, sizeof(_channel->canonical_form),
            NULL, &interface_addr,
            NULL, &endpoint_addr,
            false);
        _channel->canonical_length = strlen(_channel->canonical_form);
        _channel->is_multicast = true;
    }
    else if (NULL != _channel->uri.params.udp.control)
    {
        _channel->interface_index = 0;
        _channel->multicast_ttl = 0;
        memcpy(&_channel->remote_data, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        memcpy(&_channel->remote_control, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        memcpy(&_channel->local_data, &explicit_control_addr, AERON_ADDR_LEN(&explicit_control_addr));
        memcpy(&_channel->local_control, &explicit_control_addr, AERON_ADDR_LEN(&explicit_control_addr));
        aeron_uri_udp_canonicalise(
            _channel->canonical_form, sizeof(_channel->canonical_form),
            _channel->uri.params.udp.control, &explicit_control_addr,
            _channel->uri.params.udp.endpoint, &endpoint_addr,
            false);
        _channel->canonical_length = strlen(_channel->canonical_form);
        _channel->has_explicit_control = true;
    }
    else
    {
        if (aeron_find_unicast_interface(
                endpoint_addr.ss_family, _channel->uri.params.udp.bind_interface, &interface_addr, &interface_index) < 0)
        {
            goto error_cleanup;
        }

        _channel->interface_index = interface_index;
        _channel->multicast_ttl = 0;
        memcpy(&_channel->remote_data, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        memcpy(&_channel->remote_control, &endpoint_addr, AERON_ADDR_LEN(&endpoint_addr));
        memcpy(&_channel->local_data, &interface_addr, AERON_ADDR_LEN(&interface_addr));
        memcpy(&_channel->local_control, &interface_addr, AERON_ADDR_LEN(&interface_addr));
        aeron_uri_udp_canonicalise(
            _channel->canonical_form,
            sizeof(_channel->canonical_form),
            NULL, &interface_addr,
            _channel->uri.params.udp.endpoint, &endpoint_addr,
            has_no_distinguishing_characteristic);
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

void aeron_udp_channel_delete(const aeron_udp_channel_t *channel)
{
    if (NULL != channel)
    {
        aeron_uri_close((aeron_uri_t *)&channel->uri);
        aeron_free((void *)channel);
    }
}

extern bool aeron_udp_channel_is_wildcard(aeron_udp_channel_t *channel);
