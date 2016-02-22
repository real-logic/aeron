/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_DRIVER_MEDIA_INTERFACELOOKUP_
#define INCLUDED_AERON_DRIVER_MEDIA_INTERFACELOOKUP_

#include <functional>
#include <tuple>
#include "InetAddress.h"

namespace aeron { namespace driver { namespace media {

using IPv4Result = std::tuple<Inet4Address&, const char*, unsigned int, std::uint32_t, unsigned int>;
using IPv4LookupCallback = std::function<void(IPv4Result&)>;
using IPv6Result = std::tuple<Inet6Address&, const char*, unsigned int, std::uint32_t, unsigned int>;
using IPv6LookupCallback = std::function<void(IPv6Result&)>;

class InterfaceLookup
{
public:
    virtual void lookupIPv4(IPv4LookupCallback func) const = 0;
    virtual void lookupIPv6(IPv6LookupCallback func) const = 0;
};

class BsdInterfaceLookup : public InterfaceLookup
{
public:
    BsdInterfaceLookup(){}

    virtual void lookupIPv4(IPv4LookupCallback func) const;
    virtual void lookupIPv6(IPv6LookupCallback func) const;

    static BsdInterfaceLookup& get()
    {
        static BsdInterfaceLookup instance;
        return instance;
    }
};

}}}

#endif //AERON_INTERFACELOOKUP_H
