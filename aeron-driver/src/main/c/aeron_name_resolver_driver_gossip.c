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
#include <unistd.h>
#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "util/aeron_arrayutil.h"
#include "aeron_name_resolver.h"
#include "aeron_driver_context.h"
#include "media/aeron_udp_channel_transport_bindings.h"
#include "util/aeron_netutil.h"

// Cater for windows.
#define AERON_MAX_HOSTNAME_LEN (256)

typedef struct aeron_name_resolver_driver_stct
{
    aeron_udp_channel_transport_bindings_t *transport_bindings;

    const char *name;
    char local_addr[INET6_ADDRSTRLEN];
    struct sockaddr_storage local_socket_addr;
    const char *bootstrap_neighbor;
    struct sockaddr_storage bootstrap_neighbor_addr;
    unsigned int interface_index;
}
aeron_name_resolver_driver_t;

int aeron_name_resolver_driver_init(
    aeron_name_resolver_driver_t **driver_resolver,
    aeron_driver_context_t *context,
    const char *name,
    const char *interface_name,
    const char *bootstrap_neighbor)
{
    aeron_name_resolver_driver_t *_driver_resolver = NULL;
    char *local_hostname = NULL;

    if (aeron_alloc((void **)&_driver_resolver, sizeof(aeron_name_resolver_driver_t)) < 0)
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        goto error_cleanup;
    }

    _driver_resolver->transport_bindings = context->udp_channel_transport_bindings;

    _driver_resolver->name = name;
    if (NULL == _driver_resolver->name)
    {
        if (aeron_alloc((void **)&local_hostname, AERON_MAX_HOSTNAME_LEN) < 0)
        {
            goto error_cleanup;
        }

        if (gethostname(local_hostname, AERON_MAX_HOSTNAME_LEN) < 0)
        {
            aeron_set_err(errno, "Failed to lookup: %s", local_hostname);
            goto error_cleanup;
        }

        _driver_resolver->name = local_hostname;
    }

    if (aeron_find_unicast_interface(
        AF_INET, interface_name, &_driver_resolver->local_socket_addr, &_driver_resolver->interface_index) < 0)
    {
        goto error_cleanup;
    }

    _driver_resolver->bootstrap_neighbor = bootstrap_neighbor;
    if (NULL != _driver_resolver->bootstrap_neighbor)
    {
        if (aeron_ip_addr_resolver(name, &_driver_resolver->bootstrap_neighbor_addr, AF_INET, IPPROTO_UDP) < 0)
        {
            goto error_cleanup;
        }
    }

    *driver_resolver = _driver_resolver;

    return 0;

error_cleanup:
    aeron_free((void *)local_hostname);
    aeron_free((void *)_driver_resolver);
    return -1;
}

int aeron_name_resolver_driver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    return aeron_name_resolver_default_resolve(NULL, name, uri_param_name, is_re_resolution, address);
}

int aeron_name_resolver_driver_supplier(
    aeron_driver_context_t *context,
    aeron_name_resolver_t *resolver,
    const char *args)
{
    aeron_name_resolver_driver_t *name_resolver;

    aeron_name_resolver_driver_init(
        &name_resolver, context,
        context->resolver_name,
        context->resolver_interface,
        context->resolver_bootstrap_neighbor);

    resolver->lookup_func = aeron_name_resolver_lookup_default;
    resolver->resolve_func = aeron_name_resolver_driver_resolve;

    resolver->state = name_resolver;

    return 0;
}
