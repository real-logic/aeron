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

#include <stdlib.h>
#include <string.h>
#include "util/aeron_strutil.h"
#include "util/aeron_parse_util.h"
#include "util/aeron_netutil.h"
#include "aeron_name_resolver.h"
#include "aeron_driver_context.h"

#ifdef _MSC_VER
#define strdup _strdup
#endif

int aeron_name_resolver_init(aeron_driver_context_t *context, aeron_name_resolver_t *resolver)
{
    return context->name_resolver_supplier_func(context, resolver);
}

#define AERON_NAME_RESOLVER_STATIC_TABLE_MAX_SIZE (1024)
#define AERON_NAME_RESOLVER_STATIC_TABLE_COLUMNS (4)

typedef struct aeron_name_resolver_lookup_table_stct
{
    const char *content[AERON_NAME_RESOLVER_STATIC_TABLE_MAX_SIZE][AERON_NAME_RESOLVER_STATIC_TABLE_COLUMNS];
    size_t length;
}
aeron_name_resolver_lookup_table_t;

static aeron_name_resolver_lookup_table_t static_table_instance;

int aeron_name_resolver_lookup_static_table(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    const char **resolved_name)
{
    if (NULL == resolver->state)
    {
        return -1;
    }

    aeron_name_resolver_lookup_table_t *table = (aeron_name_resolver_lookup_table_t *)resolver->state;

    for (size_t i = 0; i < table->length; i++)
    {
        if (strcmp(name, table->content[i][0]) == 0 && strcmp(uri_param_name, table->content[i][1]) == 0)
        {
            int address_idx = is_re_resolution ? 3 : 2;
            *resolved_name = table->content[i][address_idx];
            return 1;
        }
    }

    return aeron_name_resolver_lookup_default(resolver, name, uri_param_name, is_re_resolution, resolved_name);
}

int aeron_name_resolver_supplier_static_table(aeron_driver_context_t *context, aeron_name_resolver_t *resolver)
{
    resolver->resolve_func = aeron_name_resolver_resolve_default;
    resolver->lookup_func = aeron_name_resolver_lookup_static_table;

    char *rows[AERON_NAME_RESOLVER_STATIC_TABLE_MAX_SIZE];
    char *columns[AERON_NAME_RESOLVER_STATIC_TABLE_COLUMNS];
    const char *config_str =
        &getenv(AERON_NAME_RESOLVER_SUPPLIER_ENV_VAR)[strlen(AERON_NAME_RESOLVER_STATIC_TABLE_LOOKUP_PREFIX)];
    char *config_str_dup = strdup(config_str);

    int num_rows = aeron_tokenise(config_str_dup, '|', AERON_NAME_RESOLVER_STATIC_TABLE_MAX_SIZE, rows);
    if (num_rows < 0)
    {
        return -1;
    }

    static_table_instance.length = 0;
    for (int i = num_rows; -1 < --i;)
    {
        int num_columns = aeron_tokenise(rows[i], ',', AERON_NAME_RESOLVER_STATIC_TABLE_COLUMNS, columns);
        if (AERON_NAME_RESOLVER_STATIC_TABLE_COLUMNS == num_columns)
        {
            for (int k = num_columns, l = 0; -1 < --k; l++)
            {
                static_table_instance.content[static_table_instance.length][l] = columns[k];
            }
            static_table_instance.length++;
        }
    }

    resolver->state = &static_table_instance;
    return 0;
}

int aeron_name_resolver_supplier_default(aeron_driver_context_t *context, aeron_name_resolver_t *resolver)
{
    resolver->lookup_func = aeron_name_resolver_lookup_default;
    resolver->resolve_func = aeron_name_resolver_resolve_default;
    resolver->state = NULL;
    return 0;
}

int aeron_name_resolver_resolve_default(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    return aeron_ip_addr_resolver(name, address, AF_INET, IPPROTO_UDP);
}

int aeron_name_resolver_lookup_default(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    const char **resolved_name)
{
    *resolved_name = name;
    return 0;
}

int aeron_name_resolver_resolve_host_and_port(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    struct sockaddr_storage *sockaddr)
{
    aeron_parsed_address_t parsed_address;
    const char *address_str;

    if (resolver->lookup_func(resolver, name, uri_param_name, false, &address_str) < 0)
    {
        return -1;
    }

    if (aeron_address_split(name, &parsed_address) < 0)
    {
        return -1;
    }

    const int family_hint = 6 == parsed_address.ip_version_hint ? AF_INET6 : AF_INET;

    int result = -1;
    int port = aeron_udp_port_resolver(parsed_address.port, false);

    if (0 <= port)
    {
        if (AF_INET == family_hint)
        {
            if (aeron_try_parse_ipv4(parsed_address.host, sockaddr))
            {
                result = 0;
            }
            else
            {
                result = resolver->resolve_func(resolver, parsed_address.host, uri_param_name, false, sockaddr);
            }

            ((struct sockaddr_in *)sockaddr)->sin_port = htons((uint16_t)port);
        }
        else if (AF_INET6 == family_hint)
        {
            if (aeron_try_parse_ipv6(parsed_address.host, sockaddr))
            {
                result = 0;
            }
            else
            {
                result = resolver->resolve_func(resolver, parsed_address.host, uri_param_name, false, sockaddr);
            }

            ((struct sockaddr_in6 *)sockaddr)->sin6_port = htons((uint16_t)port);
        }
    }

    return result;
}

aeron_name_resolver_supplier_func_t aeron_name_resolver_supplier_load(const char *name)
{
    if (0 == strncmp(name, AERON_NAME_RESOLVER_SUPPLIER_DEFAULT, strlen(AERON_NAME_RESOLVER_SUPPLIER_DEFAULT)))
    {
        return aeron_name_resolver_supplier_default;
    }
    else if (0 == strncmp(
        name, AERON_NAME_RESOLVER_STATIC_TABLE_LOOKUP_PREFIX, strlen(AERON_NAME_RESOLVER_STATIC_TABLE_LOOKUP_PREFIX)))
    {
        return aeron_name_resolver_supplier_static_table;
    }

    return NULL;
}
