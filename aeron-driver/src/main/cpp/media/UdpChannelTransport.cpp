/*
 * Copyright 2015 Real Logic Ltd.
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


#include <sys/errno.h>
#include <util/StringUtil.h>
#include "UdpChannelTransport.h"

using namespace aeron::driver::media;

static void setSocketOption(
    int socket, int level, int option_name, const void* option_value, socklen_t option_len)
{
    if (setsockopt(socket, level, option_name, option_value, option_len) < 0)
    {
        throw aeron::util::IOException{
            aeron::util::strPrintf("Failed to set socket option: %s", strerror(errno)), SOURCEINFO};
    }
}

void UdpChannelTransport::openDatagramChannel()
{
    const uint32_t yes = 1;
    const uint32_t no = 0;

    if (m_channel->isMulticast())
    {
        NetworkInterface& localInterface = m_channel->localInterface();
        InetAddress &dataAddress = m_channel->localData();

        int socketFd = socket(dataAddress.family(), dataAddress.type(), dataAddress.protocol());
        if (socketFd < 0)
        {
            throw aeron::util::IOException{
                aeron::util::strPrintf("Failed to open socket: %s", strerror(errno)), SOURCEINFO};
        }

        setSocketOption(socketFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        if (bind(socketFd, m_bindAddress->address(), m_bindAddress->length()) < 0)
        {
            throw aeron::util::IOException{
                aeron::util::strPrintf("Failed to bind socket: %s", strerror(errno)), SOURCEINFO};
        }

        if (dataAddress.family() == AF_INET)
        {
            ip_mreq mreq;
            memcpy(&mreq.imr_multiaddr.s_addr, dataAddress.addrPtr(), dataAddress.addrSize());
            memcpy(&mreq.imr_interface.s_addr, localInterface.address().addrPtr(), localInterface.address().addrSize());

            setSocketOption(socketFd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
        }
        else if (dataAddress.family() == AF_INET6)
        {
            ipv6_mreq mreq6;
            memcpy(&mreq6.ipv6mr_multiaddr, dataAddress.addrPtr(), dataAddress.addrSize());
            mreq6.ipv6mr_interface = localInterface.index();

            setSocketOption(socketFd, IPPROTO_IPV6, IPV6_JOIN_GROUP, &mreq6, sizeof(mreq6));
        }
    }
    else
    {

    }
}
