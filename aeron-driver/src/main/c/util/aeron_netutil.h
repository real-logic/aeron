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

#ifndef AERON_NETUTIL_H
#define AERON_NETUTIL_H

#include <stdbool.h>
#include <netinet/in.h>
#include <stdio.h>
#include <arpa/inet.h>
#include "aeron_driver_common.h"

struct ifaddrs;
struct addrinfo;

typedef int (*aeron_uri_hostname_resolver_func_t)
    (void *clientd, const char *host, struct addrinfo *hints, struct addrinfo **info);

typedef int (*aeron_getifaddrs_func_t)(struct ifaddrs **);

typedef void (*aeron_freeifaddrs_func_t)(struct ifaddrs *);

typedef int (*aeron_ifaddr_func_t)
    (void *clientd, const char *name, struct sockaddr *addr, struct sockaddr *netmask, unsigned int flags);

#define AERON_ADDR_LEN(a) (AF_INET6 == (a)->ss_family ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in))

int aeron_lookup_interfaces(aeron_ifaddr_func_t func, void *clientd);

int aeron_lookup_interfaces_from_ifaddrs(aeron_ifaddr_func_t func, void *clientd, struct ifaddrs *ifaddrs);

void aeron_set_getifaddrs(aeron_getifaddrs_func_t get_func, aeron_freeifaddrs_func_t free_func);

int aeron_host_and_port_parse_and_resolve(const char *address_str, struct sockaddr_storage *sockaddr);

int aeron_interface_parse_and_resolve(const char *interface_str, struct sockaddr_storage *sockaddr, size_t *prefixlen);

void aeron_set_ipv4_wildcard_host_and_port(struct sockaddr_storage *sockaddr);

bool aeron_ipv4_does_prefix_match(struct in_addr *in_addr1, struct in_addr *in_addr2, size_t prefixlen);

bool aeron_ipv6_does_prefix_match(struct in6_addr *in6_addr1, struct in6_addr *in6_addr2, size_t prefixlen);

size_t aeron_ipv4_netmask_to_prefixlen(struct in_addr *netmask);

size_t aeron_ipv6_netmask_to_prefixlen(struct in6_addr *netmask);

int aeron_find_interface(const char *interface_str, struct sockaddr_storage *if_addr, unsigned int *if_index);

bool aeron_is_addr_multicast(struct sockaddr_storage *addr);

bool aeron_is_wildcard_addr(struct sockaddr_storage *addr);

void aeron_format_source_identity(char *buffer, size_t length, struct sockaddr_storage *addr);

#endif //AERON_NETUTIL_H
