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

#ifndef INCLUDE_AERON_DRIVER_URI_NET_UTIL_
#define INCLUDE_AERON_DRIVER_URI_NET_UTIL_

#include <arpa/inet.h>
#include <cinttypes>

namespace aeron { namespace driver { namespace uri {

class NetUtil
{
public:
    static bool wildcardMatch(const struct in6_addr* data, const struct in6_addr* pattern, std::uint32_t prefixLength);
    static bool wildcardMatch(const struct in_addr* data, const struct in_addr* pattern, std::uint32_t prefixLength);
    static bool isEven(in_addr ipV4);
    static bool isEven(in6_addr const & ipV6);
};

}}}



#endif //AERON_NETUTIL_H
