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

#ifndef AERON_AERON_NETUTIL_H
#define AERON_AERON_NETUTIL_H

#include <netinet/in.h>

typedef int (*aeron_uri_hostname_resolver_func_t)(void *clientd, const char *host, struct addrinfo *hints, struct addrinfo **info);

typedef void (*aeron_ipv4_ifaddr_func_t)
    (unsigned int index, const char *name, struct sockaddr_in *addr, struct sockaddr_in *netmask, unsigned int flags);

typedef void (*aeron_ipv6_ifaddr_func_t)
    (unsigned int index, const char *name, struct sockaddr_in6 *addr, struct sockaddr_in6 *netmask, unsigned int flags);

int aeron_lookup_ipv4_interfaces(aeron_ipv4_ifaddr_func_t func);
int aeron_lookup_ipv6_interfaces(aeron_ipv6_ifaddr_func_t func);

void aeron_uri_hostname_resolver(aeron_uri_hostname_resolver_func_t func, void *clientd);

int aeron_host_and_port_parse_and_resolve(const char *address_str, struct sockaddr_storage *sockaddr);
int aeron_interface_parse_and_resolve(const char *interface_str, struct sockaddr_storage *sockaddr, size_t *prefixlen);

#endif //AERON_AERON_NETUTIL_H
