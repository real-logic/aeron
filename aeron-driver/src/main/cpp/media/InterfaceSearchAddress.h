//
// Created by Michael Barker on 11/09/15.
//

#ifndef INCLUDED_AERON_DRIVER_MEDIA_INTERFACESEARCHADDRESS_H_
#define INCLUDED_AERON_DRIVER_MEDIA_INTERFACESEARCHADDRESS_H_

#include "InterfaceLookup.h"
#include "InetAddress.h"

namespace aeron { namespace driver { namespace media {

class InterfaceSearchAddress
{
public:
    InterfaceSearchAddress(std::unique_ptr<InetAddress>& address, uint32_t subnetPrefix)
        : m_inetAddress(std::move(address)), m_subnetPrefix(subnetPrefix)
    {
    }

    static std::unique_ptr<InterfaceSearchAddress> parse(std::string& str);
    static std::unique_ptr<InterfaceSearchAddress> parse(const char* str)
    {
        std::string s{str};
        return parse(s);
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

    std::unique_ptr<InetAddress> findLocalAddress(InterfaceLookup& lookup) const;

private:
    std::unique_ptr<InetAddress> m_inetAddress;
    std::uint32_t m_subnetPrefix;
};

}}}

#endif //AERON_INTERFACESEARCHADDRESS_H
