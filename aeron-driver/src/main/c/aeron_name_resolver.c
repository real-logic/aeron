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

#include <errno.h>
#include <string.h>
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "util/aeron_netutil.h"
#include "util/aeron_dlopen.h"
#include "aeron_name_resolver.h"

#ifdef _MSC_VER
#define strdup _strdup
#endif

static void aeron_name_resolver_set_err(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    const char *address_str);

int aeron_name_resolver_init(aeron_name_resolver_t *resolver, const char *args, aeron_driver_context_t *context)
{
    return context->name_resolver_supplier_func(resolver, args, context);
}

int aeron_default_name_resolver_supplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context)
{
    resolver->lookup_func = aeron_default_name_resolver_lookup;
    resolver->resolve_func = aeron_default_name_resolver_resolve;
    resolver->do_work_func = aeron_default_name_resolver_do_work;
    resolver->close_func = aeron_default_name_resolver_close;
    resolver->state = NULL;

    return 0;
}

int aeron_default_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    return aeron_ip_addr_resolver(name, address, AF_INET, IPPROTO_UDP);
}

int aeron_default_name_resolver_lookup(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    const char **resolved_name)
{
    *resolved_name = name;
    return 0;
}

int aeron_default_name_resolver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    return 0;
}

int aeron_default_name_resolver_close(aeron_name_resolver_t *resolver)
{
    return 0;
}

int aeron_name_resolver_resolve_host_and_port(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *sockaddr)
{
    aeron_parsed_address_t parsed_address;
    const char *address_str = NULL;
    int result = -1;

    if (resolver->lookup_func(resolver, name, uri_param_name, is_re_resolution, &address_str) < 0)
    {
        goto exit;
    }

    if (aeron_address_split(address_str, &parsed_address) < 0)
    {
        goto exit;
    }

    const int family_hint = 6 == parsed_address.ip_version_hint ? AF_INET6 : AF_INET;

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
                result = resolver->resolve_func(
                    resolver, parsed_address.host, uri_param_name, is_re_resolution, sockaddr);
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
                result = resolver->resolve_func(
                    resolver, parsed_address.host, uri_param_name, is_re_resolution, sockaddr);
            }

            ((struct sockaddr_in6 *)sockaddr)->sin6_port = htons((uint16_t)port);
        }
    }

exit:
    if (result < 0)
    {
        aeron_name_resolver_set_err(resolver, name, uri_param_name, address_str);
    }

    return result;
}

aeron_name_resolver_supplier_func_t aeron_name_resolver_supplier_load(const char *name)
{
    aeron_name_resolver_supplier_func_t supplier_func;

    if (NULL == name)
    {
        aeron_set_err(EINVAL, "%s", "invalid name_resolver supplier function name");
        return NULL;
    }

    if (0 == strncmp(name, AERON_NAME_RESOLVER_SUPPLIER_DEFAULT, sizeof(AERON_NAME_RESOLVER_SUPPLIER_DEFAULT)))
    {
        supplier_func = aeron_default_name_resolver_supplier;
    }
    else if (0 == strncmp(name, AERON_NAME_RESOLVER_CSV_TABLE, sizeof(AERON_NAME_RESOLVER_CSV_TABLE)))
    {
        supplier_func = aeron_name_resolver_supplier_load("aeron_csv_table_name_resolver_supplier");
    }
    else if (0 == strncmp(name, AERON_NAME_RESOLVER_DRIVER, sizeof(AERON_NAME_RESOLVER_DRIVER)))
    {
        supplier_func = aeron_name_resolver_supplier_load("aeron_driver_name_resolver_supplier");
    }
    else
    {
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
        if ((supplier_func = (aeron_name_resolver_supplier_func_t)aeron_dlsym(RTLD_DEFAULT, name)) == NULL)
        {
            aeron_set_err(
                EINVAL, "could not find name resolver %s: dlsym - %s", name, aeron_dlerror());
        }
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif
    }

    return supplier_func;
}

static void aeron_name_resolver_set_err(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    const char *address_str)
{
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
    char dl_name_buffer[128];
    const char *address_or_null = NULL != address_str ? address_str : "null";
    aeron_set_err(
        EINVAL,
        "Unresolved - %s=%s, name-and-port=%s, name-resolver-lookup=%s, name-resolver-resolve=%s",
        uri_param_name,
        name,
        address_or_null,
        aeron_dlinfo((const void *)resolver->lookup_func, dl_name_buffer, sizeof(dl_name_buffer)),
        aeron_dlinfo((const void *)resolver->resolve_func, dl_name_buffer, sizeof(dl_name_buffer)));
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif
}
