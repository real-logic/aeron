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

#include <errno.h>
#include <string.h>

#include "aeron_http_util.h"
#include "aeron_error.h"
#include "aeron_netutil.h"
#include "aeron_socket.h"

int aeron_http_parse_url(const char *url, aeron_http_parsed_url_t *parsed_url)
{
    const char *pb = url;
    char c;
    int i = 0, at_index = -1, first_slash_index = -1, end_index = -1, length;

    if (strncmp(url, "http://", strlen("http://")) != 0)
    {
        aeron_set_err(EINVAL, "URL %s does not have supported scheme", url);
        return -1;
    }

    pb += strlen("http://");

    while ((c = *(pb + i)) != '\0')
    {
        if ('@' == c)
        {
            at_index = i;
        }
        else if (-1 == first_slash_index && '/' == c)
        {
            first_slash_index = i;
        }

        i++;
    }
    end_index = i - 1;

    parsed_url->userinfo[0] = '\0';
    parsed_url->host_and_port[0] = '\0';
    parsed_url->path_and_query[0] = '\0';

    if (-1 != first_slash_index)
    {
        length = end_index - first_slash_index;

        if (length > (int)sizeof(parsed_url->path_and_query))
        {
            aeron_set_err(EINVAL, "URL %s has too long path", url);
            return -1;
        }

        memcpy(parsed_url->path_and_query, pb + first_slash_index, (size_t)length);
        parsed_url->path_and_query[length] = '\0';
        end_index = first_slash_index;
    }

    if (-1 != at_index)
    {
        length = at_index;

        if (length > (int)sizeof(parsed_url->userinfo))
        {
            aeron_set_err(EINVAL, "URL %s has too long userinfo", url);
            return -1;
        }

        memcpy(parsed_url->userinfo, pb, (size_t)length);
        parsed_url->userinfo[length] = '\0';
        pb += length;
    }

    length = end_index - (-1 == at_index ? 0 : at_index);

    if (length > (int)sizeof(parsed_url->host_and_port))
    {
        aeron_set_err(EINVAL, "URL %s has too long host and port", url);
        return -1;
    }

    memcpy(parsed_url->host_and_port, pb, (size_t)length);
    parsed_url->host_and_port[length] = '\0';

    aeron_parsed_address_t parsed_address;

    if (-1 == aeron_address_split(parsed_url->host_and_port, &parsed_address))
    {
        return -1;
    }

    int port = aeron_udp_port_resolver(parsed_address.port, true);

    port = 0 == port ? 80 : port;

    int result = 0;
    if (6  == parsed_address.ip_version_hint)
    {
        result = aeron_ipv6_addr_resolver(parsed_address.host, IPPROTO_TCP, &parsed_url->address);
        ((struct sockaddr_in6 *)&parsed_url->address)->sin6_port = htons((uint16_t)port);
    }
    else
    {
        result = aeron_ipv4_addr_resolver(parsed_address.host, IPPROTO_TCP, &parsed_url->address);
        ((struct sockaddr_in *)&parsed_url->address)->sin_port = htons((uint16_t)port);
    }

    parsed_url->ip_version_hint = parsed_address.ip_version_hint;

    return result;
}

int aeron_http_retrieve(const char **content, const char *url, int64_t timeout_ns)
{
    aeron_http_parsed_url_t parsed_url;
    int sock;

    if (aeron_http_parse_url(url, &parsed_url) == -1)
    {
        return -1;
    }

    if ((sock = aeron_socket(parsed_url.address.ss_family, SOCK_STREAM, 0)) == -1)
    {
        return -1;
    }

    bool is_ipv6 = (AF_INET6 == parsed_url.address.ss_family);
    socklen_t addr_len = is_ipv6 ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);

    if (connect(sock, (struct sockaddr *)&parsed_url.address, addr_len) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "http connect: %s", strerror(errcode));
        goto error;
    }


    error:
        if (-1 != sock)
        {
            aeron_close_socket(sock);
        }

        return -1;
}
