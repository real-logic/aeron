//
// Created by Michael Barker on 01/09/15.
//

#ifndef INCLUDE_AERON_DRIVER_URI_NET_UTIL_
#define INCLUDE_AERON_DRIVER_URI_NET_UTIL_

#include <arpa/inet.h>
#include <cinttypes>

namespace aeron { namespace driver { namespace uri {

class NetUtil
{
public:
    static bool wildcardMatch(struct in6_addr* data, struct in6_addr* pattern, std::uint32_t prefixLength);
    static bool wildcardMatch(struct in_addr* data, struct in_addr* pattern, std::uint32_t prefixLength);
};

}}}



#endif //AERON_NETUTIL_H
