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

#ifndef INCLUDE_AERON_DRIVER_MEDIA_INTERFACE_LOOKUP_
#define INCLUDE_AERON_DRIVER_MEDIA_INTERFACE_LOOKUP_

#include <functional>
#include "InetAddress.h"

namespace aeron { namespace driver { namespace media {

class InterfaceLookup
{
public:
    virtual void lookupIPv4(std::function<void(Inet4Address&, std::uint32_t, unsigned int)> func) const = 0;
    virtual void lookupIPv6(std::function<void(Inet6Address&, std::uint32_t, unsigned int)> func) const = 0;
};

class BsdInterfaceLookup : public InterfaceLookup
{
public:
    BsdInterfaceLookup(){}

    virtual void lookupIPv4(std::function<void(Inet4Address&, std::uint32_t, unsigned int)> func) const;
    virtual void lookupIPv6(std::function<void(Inet6Address&, std::uint32_t, unsigned int)> func) const;

    static BsdInterfaceLookup& get()
    {
        static BsdInterfaceLookup instance;
        return instance;
    }

//    template<typename Func>
//    virtual void lookupIPv6(Func) const;
};

}}}

#endif //AERON_INTERFACELOOKUP_H
