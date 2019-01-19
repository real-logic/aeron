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

#include <memory.h>
#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include <ctype.h>
#include "util/aeron_parse_util.h"
#include "util/aeron_error.h"

const uint64_t AERON_MAX_G_VALUE = 8589934591ULL;
const uint64_t AERON_MAX_M_VALUE = 8796093022207ULL;
const uint64_t AERON_MAX_K_VALUE = 9007199254739968ULL;

int aeron_parse_size64(const char *str, uint64_t *result)
{
    if (NULL == str)
    {
        return -1;
    }

    errno = 0;
    char *end = "";
    const int64_t v = strtoll(str, &end, 10);

    if (0 == v && 0 != errno)
    {
        return -1;
    }

    if (v < 0 || end == str)
    {
        return -1;
    }

    const uint64_t value = (uint64_t)v;

    if ('\0' != *end)
    {
        switch (*end)
        {
            case 'k':
            case 'K':
                if (value > AERON_MAX_K_VALUE)
                {
                    return -1;
                }
                *result = value * 1024;
                break;

            case 'm':
            case 'M':
                if (value > AERON_MAX_M_VALUE)
                {
                    return -1;
                }
                *result = value * 1024 * 1024;
                break;

            case 'g':
            case 'G':
                if (value > AERON_MAX_G_VALUE)
                {
                    return -1;
                }
                *result = value * 1024 * 1024 * 1024;
                break;

            default:
                return -1;
        }
    }
    else
    {
        *result = value;
    }

    return 0;
}

int aeron_parse_duration_ns(const char *str, uint64_t *result)
{
    if (NULL == str)
    {
        return -1;
    }

    errno = 0;
    char *end = "";
    const int64_t v = strtoll(str, &end, 10);

    if (0 == v && 0 != errno)
    {
        return -1;
    }

    if (v < 0 || end == str)
    {
        return -1;
    }

    const uint64_t value = (uint64_t)v;

    if ('\0' != *end)
    {
        switch (tolower(*end))
        {
            case 's':
                if ('\0' != *(end + 1))
                {
                    return -1;
                }

                if (value > LLONG_MAX / 1000000000)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * 1000000000;
                }
                break;

            case 'm':
                if (tolower(*(end + 1)) != 's' && '\0' != *(end + 2))
                {
                    return -1;
                }

                if (value > LLONG_MAX / 1000000)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * 1000000;
                }
                break;

            case 'u':
                if (tolower(*(end + 1)) != 's' && '\0' != *(end + 2))
                {
                    return -1;
                }

                if (value > LLONG_MAX / 1000)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * 1000;
                }
                break;

            case 'n':
                if (tolower(*(end + 1)) != 's' && '\0' != *(end + 2))
                {
                    return -1;
                }

                *result = value;
                break;

            default:
                return -1;
        }
    }
    else
    {
        *result = value;
    }

    return 0;
}


int aeron_address_split(const char *address_str, aeron_parsed_address_t *parsed_address)
{
    if (NULL == address_str || '\0' == *address_str)
    {
        aeron_set_err(EINVAL, "no address value");
        return -1;
    }

    int percent_index = -1;
    int colon_index = -1;
    int l_brace_index = -1;
    int r_brace_index = -1;

    int i = 0;
    char c = *address_str;
    while ('\0' != c)
    {
        if (':' == c)
        {
            colon_index = i;
        }
        else if ('[' == c)
        {
            l_brace_index = i;
        }
        else if (']' == c)
        {
            r_brace_index = i;
        }
        else if ('%'  == c)
        {
            percent_index = i;
        }

        ++i;
        c = *(address_str + i);
    }

    bool is_ipv6 = false;
    if (l_brace_index > 0 || r_brace_index > 0)
    {
        if (l_brace_index < 0 || r_brace_index < 0 || r_brace_index < l_brace_index)
        {
            aeron_set_err(EINVAL, "host address invalid: %s", address_str);
            return -1;
        }

        is_ipv6 = true;
        parsed_address->ip_version_hint = 6;
    }
    else
    {
        parsed_address->ip_version_hint = 4;
    }

    *parsed_address->port = '\0';
    if (colon_index >= 0 && r_brace_index < colon_index)
    {
        if (i - 1 == colon_index)
        {
            aeron_set_err(EINVAL, "port invalid: %s", address_str);
            return -1;
        }

        int port_begin = colon_index + 1;
        int length = i - port_begin;

        if (length >= AERON_MAX_PORT_LENGTH)
        {
            aeron_set_err(EINVAL, "port invalid: %s", address_str);
            return -1;
        }

        strncpy(parsed_address->port, address_str + port_begin, (size_t)length);
        *(parsed_address->port + length) = '\0';
    }

    int length = i;

    if (colon_index >= 0 && colon_index > r_brace_index)
    {
        length = colon_index;
    }

    bool is_scoped = false;
    if (percent_index >= 0 && percent_index < r_brace_index)
    {
        is_scoped = true;
        length = percent_index;
    }

    const char *host = is_ipv6 ? address_str + 1 : address_str;
    if (is_ipv6)
    {
        length =  is_scoped ? length -1 : length - 2;
    }

    if (length >= AERON_MAX_HOST_LENGTH)
    {
        aeron_set_err(EINVAL, "host address invalid: %s", address_str);
        return -1;
    }

    strncpy(parsed_address->host, host, (size_t)length);
    *(parsed_address->host + length) = '\0';

    return 0;
}

int aeron_interface_split(const char *interface_str, aeron_parsed_interface_t *parsed_interface)
{
    if (NULL == interface_str || '\0' == *interface_str)
    {
        aeron_set_err(EINVAL, "no interface value");
        return -1;
    }

    int percent_index = -1;
    int colon_index = -1;
    int l_brace_index = -1;
    int r_brace_index = -1;
    int slash_index = -1;

    int i = 0;
    char c = *interface_str;
    while ('\0' != c)
    {
        if (':' == c)
        {
            colon_index = i;
        }
        else if ('[' == c)
        {
            l_brace_index = i;
        }
        else if (']' == c)
        {
            r_brace_index = i;
        }
        else if ('/'  == c)
        {
            slash_index = i;
        }
        else if ('%'  == c)
        {
            percent_index = i;
        }

        ++i;
        c = *(interface_str + i);
    }

    bool is_ipv6 = false;
    if (l_brace_index > 0 || r_brace_index > 0)
    {
        if (l_brace_index < 0 || r_brace_index < 0 || r_brace_index < l_brace_index)
        {
            aeron_set_err(EINVAL, "host address invalid: %s", interface_str);
            return -1;
        }

        is_ipv6 = true;
        parsed_interface->ip_version_hint = 6;
    }
    else
    {
        parsed_interface->ip_version_hint = 4;
    }

    *parsed_interface->prefix = '\0';
    if (slash_index >= 0)
    {
        int length = i - slash_index;
        if (length <= 0)
        {
            aeron_set_err(EINVAL, "subnet prefix invalid: %s", interface_str);
            return -1;
        }

        if (length >= AERON_MAX_PREFIX_LENGTH)
        {
            aeron_set_err(EINVAL, "subnet prefix invalid: %s", interface_str);
            return -1;
        }

        strncpy(parsed_interface->prefix, interface_str + slash_index + 1, (size_t)length);
        *(parsed_interface->prefix + length) = '\0';
    }

    *parsed_interface->port = '\0';
    if (colon_index >= 0 && r_brace_index < colon_index)
    {
        if (i - 1 == colon_index)
        {
            aeron_set_err(EINVAL, "port invalid: %s", interface_str);
            return -1;
        }

        int port_begin = colon_index + 1;
        int length = slash_index > 0 ? slash_index - port_begin : i - port_begin;

        if (length >= AERON_MAX_PORT_LENGTH)
        {
            aeron_set_err(EINVAL, "port invalid: %s", interface_str);
            return -1;
        }

        strncpy(parsed_interface->port, interface_str + port_begin, (size_t)length);
        *(parsed_interface->port + length) = '\0';
    }

    int length = i;

    if (slash_index >= 0)
    {
        length = slash_index;
    }

    if (colon_index >= 0 && colon_index > r_brace_index)
    {
        length = colon_index;
    }

    bool is_scoped = false;
    if (percent_index >= 0 && percent_index < r_brace_index)
    {
        length = percent_index;
        is_scoped = true;
    }

    const char *host = is_ipv6 ? interface_str + 1 : interface_str;
    if (is_ipv6)
    {
        length =  is_scoped ? length -1 : length - 2;
    }

    if (length >= AERON_MAX_HOST_LENGTH)
    {
        aeron_set_err(EINVAL, "host address invalid: %s", interface_str);
        return -1;
    }

    strncpy(parsed_interface->host, host, (size_t)length);
    *(parsed_interface->host + length) = '\0';

    return 0;
}


extern int aeron_parse_size64(const char *str, uint64_t *result);

extern int aeron_parse_duration_ns(const char *str, uint64_t *result);

extern int aeron_address_split(const char *address_str, aeron_parsed_address_t *parsed_address);

extern int aeron_interface_split(const char *interface_str, aeron_parsed_interface_t *parsed_interface);