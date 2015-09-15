//
// Created by Michael Barker on 14/09/15.
//

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
