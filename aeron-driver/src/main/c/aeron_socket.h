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

#ifndef AERON_SOCKET_H
#define AERON_SOCKET_H

#include <util/aeron_platform.h>
#include <stdint.h>

typedef int aeron_fd_t;
int set_socket_non_blocking(aeron_fd_t fd);
int aeron_socket(int domain, int type, int protocol);
void aeron_close_socket(int socket);
void aeron_net_init();

#if defined(AERON_COMPILER_GCC)
    #include <netinet/in.h>
    #include <sys/socket.h>
    #include <net/if.h>
    #include <netinet/ip.h>
    #include <arpa/inet.h>
    #include <netdb.h>
    #include <ifaddrs.h>

#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
    #include <WinSock2.h>
    #include <windows.h>
    #include <Ws2ipdef.h>
    #include <WS2tcpip.h>
    #include <Iphlpapi.h>

    struct iovec
    {
        ULONG iov_len;
        void* iov_base;
    };

    // must match _WSAMSG 
    struct msghdr {
        void* msg_name;
        INT msg_namelen;		
        struct iovec *msg_iov;	
        ULONG msg_iovlen;		
        ULONG msg_controllen;
        void* msg_control;
        ULONG msg_flags;		
    };

    struct ifaddrs
    {
        struct ifaddrs *ifa_next; 
        char *ifa_name; 
        unsigned int ifa_flags;

        struct sockaddr *ifa_addr; 
        struct sockaddr *ifa_netmask; 
        union
        {
         
            struct sockaddr *ifu_broadaddr; 
            struct sockaddr *ifu_dstaddr;
        } ifa_ifu;

        # ifndef ifa_broadaddr
        #  define ifa_broadaddr        ifa_ifu.ifu_broadaddr
        # endif
        # ifndef ifa_dstaddr
        #  define ifa_dstaddr        ifa_ifu.ifu_dstaddr
        # endif

        void *ifa_data;             
    };

    int getifaddrs(struct ifaddrs **__ifap);
    void freeifaddrs(struct ifaddrs *__ifa);


#else
#error Unsupported platform!
#endif

#endif //AERON_SOCKET_H
