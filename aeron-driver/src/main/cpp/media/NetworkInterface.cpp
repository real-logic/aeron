//
// Created by Michael Barker on 07/10/15.
//

#include <util/Exceptions.h>
#include "NetworkInterface.h"

using namespace aeron::driver::media;
using namespace aeron::util;

void NetworkInterface::setAsMulticastInterface(int socketFd) const
{
    switch (m_address->family())
    {
        case AF_INET:
            if(setsockopt(socketFd, IPPROTO_IP, IP_MULTICAST_IF, m_address->addrPtr(), m_address->addrSize()) < 0)
            {
                throw IOException("Unable to set socket option", SOURCEINFO);
            }
            break;
        case AF_INET6:
            break;
        default:
            throw IllegalStateException("Invalid address type", SOURCEINFO);
    }
}
