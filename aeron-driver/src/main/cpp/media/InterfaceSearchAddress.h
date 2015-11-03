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

#ifndef INCLUDED_AERON_DRIVER_MEDIA_INTERFACESEARCHADDRESS_H_
#define INCLUDED_AERON_DRIVER_MEDIA_INTERFACESEARCHADDRESS_H_

#include "InterfaceLookup.h"
#include "InetAddress.h"
#include "NetworkInterface.h"

namespace aeron { namespace driver { namespace media {

class InterfaceSearchAddress
{
public:
    InterfaceSearchAddress(std::unique_ptr<InetAddress>& address, uint32_t subnetPrefix)
        : m_inetAddress(std::move(address)), m_subnetPrefix(subnetPrefix)
    {
    }

    static std::unique_ptr<InterfaceSearchAddress> parse(std::string& str, int familyHint = PF_INET);
    static std::unique_ptr<InterfaceSearchAddress> parse(const char* str, int familyHint = PF_INET)
    {
        std::string s{str};
        return parse(s, familyHint);
    }

    InetAddress& inetAddress() const
    {
        return *m_inetAddress;
    }

    std::uint32_t subnetPrefix() const
    {
        return m_subnetPrefix;
    }

    bool matches(const InetAddress& candidate) const;

    std::unique_ptr<NetworkInterface> findLocalAddress(InterfaceLookup& lookup) const;

private:
    std::unique_ptr<InetAddress> m_inetAddress;
    std::uint32_t m_subnetPrefix;
};

}}}

#endif //AERON_INTERFACESEARCHADDRESS_H
