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

#include <netinet/in.h>
#include <regex.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdlib.h>
#include "uri/aeron_uri.h"

typedef enum aeron_uri_parser_state_enum
{
    PARAM_KEY, PARAM_VALUE
}
aeron_uri_parser_state_t;

int aeron_uri_parse(char *uri, aeron_uri_parse_callback_t param_func, void *clientd)
{
    aeron_uri_parser_state_t state = PARAM_KEY;
    char *param_key = NULL, *param_value = NULL;

    for (size_t i = 0; uri[i] != '\0'; i++)
    {
        char c = uri[i];

        switch (state)
        {
            case PARAM_KEY:
                switch (c)
                {
                    case '=':
                        uri[i] = '\0';
                        param_value = NULL;
                        state = PARAM_VALUE;
                        break;

                    default:
                        if (NULL == param_key)
                        {
                            param_key = &uri[i];
                        }
                        break;
                }
                break;

            case PARAM_VALUE:
                switch (c)
                {
                    case '|':
                        uri[i] = '\0';
                        if (param_func(clientd, param_key, param_value) < 0)
                        {
                            return -1;
                        }

                        param_key = NULL;
                        state = PARAM_KEY;
                        break;

                    default:
                        if (NULL == param_value)
                        {
                            param_value = &uri[i];
                        }
                        break;
                }
                break;
        }
    }

    if (state == PARAM_VALUE)
    {
        if (param_func(clientd, param_key, param_value) < 0)
        {
            return -1;
        }
    }

    return 0;
}

static int aeron_udp_uri_params_func(void *clientd, const char *key, const char *value)
{
    aeron_udp_channel_params_t *params = (aeron_udp_channel_params_t *)clientd;

    if (strcmp(key, AERON_UDP_CHANNEL_ENDPOINT_KEY) == 0)
    {
        params->endpoint_key = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_INTERFACE_KEY) == 0)
    {
        params->interface_key = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_TTL_KEY) == 0)
    {
        params->ttl_key = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_CONTROL_KEY) == 0)
    {
        params->control_key = value;
    }
    else
    {
        size_t index = params->additional_params.length;
        if (aeron_uri_params_ensure_capacity(&params->additional_params) < 0)
        {
            return -1;
        }

        aeron_uri_param_t *param = &params->additional_params.array[index];

        param->key = key;
        param->value = value;
    }

    return 0;
}

int aeron_udp_uri_parse(char *uri, aeron_udp_channel_params_t *params)
{
    params->additional_params.length = 0;
    params->additional_params.array = NULL;
    params->endpoint_key = NULL;
    params->interface_key = NULL;
    params->ttl_key = NULL;
    params->control_key = NULL;

    return aeron_uri_parse(uri, aeron_udp_uri_params_func, params);
}

static aeron_uri_hostname_resolver_func_t aeron_uri_hostname_resolver_func = NULL;

void aeron_uri_hostname_resolver(aeron_uri_hostname_resolver_func_t func)
{
    aeron_uri_hostname_resolver_func = func;
}

int aeron_ip_addr_resolver(const char *host, struct sockaddr_storage *sockaddr, int family_hint)
{
    struct addrinfo hints;
    struct addrinfo *info = NULL;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = family_hint;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;

    int error, result = -1;
    if ((error = getaddrinfo(host, NULL, &hints, &info)) != 0)
    {
        if (NULL == aeron_uri_hostname_resolver_func)
        {
            aeron_set_err(EINVAL, "Unable to resolve host=(%s): (%d) %s", host, error, gai_strerror(error));
            return -1;
        }
        else if (aeron_uri_hostname_resolver_func(host, &hints, info) != 0)
        {
            aeron_set_err(EINVAL, "Unable to resolve host=(%s): %s", host, aeron_errmsg());
            return -1;
        }
    }

    if (info->ai_family == AF_INET)
    {
        memcpy(sockaddr, &info->ai_addr, sizeof(struct sockaddr_in));
        sockaddr->ss_family = AF_INET;
        sockaddr->ss_len = sizeof(struct sockaddr_in);
        result = 0;
    }
    else if (info->ai_family == AF_INET6)
    {
        memcpy(sockaddr, &info->ai_addr, sizeof(struct sockaddr_in6));
        sockaddr->ss_family = AF_INET6;
        sockaddr->ss_len = sizeof(struct sockaddr_in6);
        result = 0;
    }
    else
    {
        aeron_set_err(EINVAL, "Only IPv4 and IPv6 hosts are supported: family=%d", info->ai_family);
    }

    freeaddrinfo(info);
    return result;
}

int aeron_ipv4_addr_resolver(const char *host, struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in *addr = (struct sockaddr_in *)sockaddr;

    if (inet_pton(AF_INET, host, &addr->sin_addr))
    {
        sockaddr->ss_family = AF_INET;
        sockaddr->ss_len = sizeof(struct sockaddr_in);
        return 0;
    }

    return aeron_ip_addr_resolver(host, sockaddr, AF_INET);
}

int aeron_ipv6_addr_resolver(const char *host, struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in6 *addr = (struct sockaddr_in6 *)sockaddr;

    if (inet_pton(AF_INET6, host, &addr->sin6_addr))
    {
        sockaddr->ss_family = AF_INET6;
        sockaddr->ss_len = sizeof(struct sockaddr_in6);
        return 0;
    }

    return aeron_ip_addr_resolver(host, sockaddr, AF_INET6);
}

int aeron_udp_port_resolver(const char *port_str)
{
    unsigned long value = strtoul(port_str, NULL, 0);

    if (0 == value && EINVAL == errno)
    {
        aeron_set_err(EINVAL, "port invalid: %s", port_str);
        return -1;
    }
    else if (value >= UINT16_MAX)
    {
        aeron_set_err(EINVAL, "port out of range: %s", port_str);
        return -1;
    }

    return (int)value;
}

int aeron_host_and_port_resolver(
    const char *host_str, const char *port_str, struct sockaddr_storage *sockaddr, int family_hint)
{
    int result = -1, port = aeron_udp_port_resolver(port_str);

    if (port >= 0)
    {
        if (AF_INET == family_hint)
        {
            ((struct sockaddr_in *) sockaddr)->sin_port = htons(port);
            result = aeron_ipv4_addr_resolver(host_str, sockaddr);
        }
        else if (AF_INET6 == family_hint)
        {
            ((struct sockaddr_in6 *) sockaddr)->sin6_port = htons(port);
            result = aeron_ipv6_addr_resolver(host_str, sockaddr);
        }
    }

    return result;
}

int aeron_host_and_port_parse(const char *address_str, struct sockaddr_storage *sockaddr)
{
    static bool regexs_compiled = false;
    static regex_t ipv4_regex, ipv6_regex;
    regmatch_t matches[4];

    if (!regexs_compiled)
    {
        const char *ipv4 = "([^:]+)(?::([0-9]+))?";
        const char *ipv6 = "\\[([0-9A-Fa-f:]+)(?:%([a-zA-Z0-9_.~-]+))?\\](?::([0-9]+))?";

        int regcomp_result;
        if ((regcomp_result = regcomp(&ipv4_regex, ipv4, 0)) != 0)
        {
            char message[AERON_MAX_PATH];

            regerror(regcomp_result, &ipv4_regex, message, sizeof(message));
            aeron_set_err(EINVAL, "could not regcomp IPv4 regex: %s", message);
            return -1;
        }

        if ((regcomp_result = regcomp(&ipv6_regex, ipv6, 0)) != 0)
        {
            char message[AERON_MAX_PATH];

            regerror(regcomp_result, &ipv6_regex, message, sizeof(message));
            aeron_set_err(EINVAL, "could not regcomp IPv6 regex: %s", message);
            return -1;
        }

        regexs_compiled = true;
    }

    int regexec_result = regexec(&ipv4_regex, address_str, 3, matches, 0);
    if (0 == regexec_result)
    {
        char host[AERON_MAX_PATH], port[AERON_MAX_PATH];

        strncpy(host, &address_str[matches[1].rm_so], matches[1].rm_eo - matches[1].rm_so);
        strncpy(port, &address_str[matches[2].rm_so], matches[2].rm_eo - matches[2].rm_so);

        return aeron_host_and_port_resolver(host, port, sockaddr, AF_INET);
    }
    else if (REG_NOMATCH != regexec_result)
    {
        char message[AERON_MAX_PATH];

        regerror(regexec_result, &ipv4_regex, message, sizeof(message));
        aeron_set_err(EINVAL, "could not regexec IPv4 regex: %s", message);
        return -1;
    }

    regexec_result = regexec(&ipv6_regex, address_str, 4, matches, 0);
    if (0 == regexec_result)
    {
        char host[AERON_MAX_PATH], port[AERON_MAX_PATH];

        strncpy(host, &address_str[matches[1].rm_so], matches[1].rm_eo - matches[1].rm_so);
        strncpy(port, &address_str[matches[2].rm_so], matches[2].rm_eo - matches[2].rm_so);

        return aeron_host_and_port_resolver(host, port, sockaddr, AF_INET6);
    }
    else if (REG_NOMATCH != regexec_result)
    {
        char message[AERON_MAX_PATH];

        regerror(regexec_result, &ipv4_regex, message, sizeof(message));
        aeron_set_err(EINVAL, "could not regexec IPv6 regex: %s", message);
        return -1;
    }

    aeron_set_err(EINVAL, "invalid format: %s", address_str);
    return -1;
}

extern int aeron_uri_params_ensure_capacity(aeron_uri_params_t *params);
