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
#include <stdlib.h>
#include <inttypes.h>

#include "aeron_http_util.h"
#include "aeron_error.h"
#include "aeron_netutil.h"
#include "aeron_socket.h"
#include "aeron_alloc.h"
#include "aeron_arrayutil.h"
#include "concurrent/aeron_thread.h"
#include "aeronmd.h"

int aeron_http_parse_url(const char *url, aeron_http_parsed_url_t *parsed_url)
{
    const char *pb = url;
    char c;
    int i = 0, at_index = -1, first_slash_index = -1, end_index, length;

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
    end_index = i;

    parsed_url->userinfo[0] = '\0';
    parsed_url->host_and_port[0] = '\0';
    parsed_url->path_and_query[0] = '/';
    parsed_url->path_and_query[1] = '\0';

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

int aeron_http_response_ensure_capacity(aeron_http_response_t *response, size_t new_capacity)
{
    if (new_capacity > response->capacity)
    {
        new_capacity = aeron_find_next_power_of_two((int32_t)new_capacity);

        if (aeron_array_ensure_capacity((uint8_t **)&response->buffer, 1, response->capacity, new_capacity) < 0)
        {
            return -1;
        }

        response->capacity = new_capacity;

        return 0;
    }

    return 0;
}

bool aeron_http_response_is_complete(aeron_http_response_t *response)
{
    char line[AERON_HTTP_MAX_HEADER_LENGTH];
    int line_result = 0;

    if (0 == response->status_code)
    {
        if ((line_result = aeron_parse_get_line(line, sizeof(line), response->buffer)) == -1)
        {
            response->parse_err = true;
            return true;
        }
        else if (0 < line_result)
        {
            char version[8], code_str[4], reason_phrase[1024];

            int matches = sscanf(line, "%8s %3[0-9] %s\r\n", version, code_str, reason_phrase);

            if (3 == matches)
            {
                errno = 0;
                unsigned long code = strtoul(code_str, NULL, 10);

                if (0 == code)
                {
                    aeron_set_err(EINVAL, "http response code <%s> parsed to 0, errno=%d", code_str, errno);
                    return true;
                }

                response->status_code = (size_t)code;
                response->cursor = line_result;
                response->headers_offset = line_result;
            }
            else
            {
                aeron_set_err(EINVAL, "could not parse response line: <%s>", line);
                response->parse_err = true;
            }
        }
    }
    else if (0 == response->body_offset)
    {
        if ((line_result = aeron_parse_get_line(line, sizeof(line), response->buffer + response->cursor)) == -1)
        {
            response->parse_err = true;
            return true;
        }
        else if (0 < line_result)
        {
            if (strncmp(line, "\r\n", 2) == 0)
            {
                response->body_offset = response->cursor + line_result;
            }
            else if (strncmp(line, "Content-Length:", strlen("Content-Length:")) == 0)
            {
                errno = 0;
                unsigned long content_length = strtoul(line + strlen("Content-Length:"), NULL, 10);

                if (0 == content_length)
                {
                    aeron_set_err(EINVAL, "http Content-Length <%s> parsed to 0, errno=%d", line, errno);
                    return true;
                }

                response->content_length = (size_t)content_length;
            }

            response->cursor += line_result;
        }
    }
    else
    {
        if (0 < response->content_length && response->content_length <= (response->length - response->body_offset))
        {
            return true;
        }
    }

    return false;
}

static char aeron_http_request_format[] =
    "GET %s HTTP/1.1\r\n"
    "Host: %s\r\n"
    "Accept: text/plain, text/*, */*\r\n"
    "Accept-Encoding: identity\r\n"
    "\r\n";

int aeron_http_retrieve(aeron_http_response_t **response, const char *url, int64_t timeout_ns)
{
    aeron_http_parsed_url_t parsed_url;
    int sock;
    aeron_http_response_t *_response = NULL;

    *response = NULL;
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

    char request[sizeof(parsed_url.path_and_query) + sizeof(aeron_http_request_format) + 1];
    int length = snprintf(
        request, sizeof(request) - 1, aeron_http_request_format, parsed_url.path_and_query, parsed_url.host_and_port);
    ssize_t sent_length = 0;

    if (length < 0 || (sent_length = send(sock, request, (size_t)length, 0)) < length)
    {
        int errcode = errno;

        aeron_set_err(errcode, "http sent %" PRId64 "/%d bytes: %s", (uint64_t)sent_length, length, strerror(errcode));
        goto error;
    }

    if (set_socket_non_blocking(sock) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "http set_socket_non_blocking: %s", strerror(errcode));
        goto error;
    }

    if (aeron_alloc((void **)&_response, sizeof(aeron_http_response_t)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "http alloc response: %s", strerror(errcode));
        goto error;
    }

    _response->buffer = NULL;
    _response->headers_offset = 0;
    _response->cursor = 0;
    _response->body_offset = 0;
    _response->length = 0;
    _response->capacity = 0;
    _response->status_code = 0;
    _response->content_length = 0;
    _response->parse_err = false;

    const int64_t start_ns = aeron_nano_clock();

    do
    {
        const int64_t now_ns = aeron_nano_clock();

        if (-1 != timeout_ns && now_ns > (start_ns + timeout_ns))
        {
            aeron_set_err(ETIMEDOUT, "http recv timeout: %s", strerror(ETIMEDOUT));
            goto error;
        }

        if (aeron_http_response_ensure_capacity(_response, _response->length + AERON_HTTP_RESPONSE_RECV_LENGTH + 1) < 0)
        {
            goto error;
        }

        ssize_t recv_length = 0;
        if ((recv_length = recv(sock, _response->buffer + _response->length, AERON_HTTP_RESPONSE_RECV_LENGTH, 0)) < 0)
        {
            int errcode = errno;

            if (EINTR == errcode || EAGAIN == errcode)
            {
                sched_yield();
                continue;
            }

            aeron_set_err(errcode, "http recv: %s", strerror(errcode));
            goto error;
        }

        if (0 == recv_length)
        {
            break;
        }

        _response->length += recv_length;
        _response->buffer[_response->length] = '\0';
    }
    while (!aeron_http_response_is_complete(_response));

    if (_response->parse_err)
    {
        goto error;
    }

    *response = _response;
    return 0;

    error:
        if (-1 != sock)
        {
            aeron_close_socket(sock);
        }

        aeron_http_response_delete(_response);

        return -1;
}

int aeron_http_header_get(aeron_http_response_t *response, const char *header_name, char *line, size_t max_length)
{
    int line_result = 0, header_name_length = strlen(header_name);
    size_t cursor = response->headers_offset;

    while (cursor < response->body_offset)
    {
        if ((line_result = aeron_parse_get_line(line, max_length, response->buffer + cursor)) == -1)
        {
            return -1;
        }
        else if (0 == line_result)
        {
            break;
        }

        if (strncmp(line, header_name, header_name_length) == 0)
        {
            return 1;
        }

        cursor += line_result;
    }

    line[0] = '\0';

    return 0;
}

extern void aeron_http_response_delete(aeron_http_response_t *response);
