//
// Created by Michael Barker on 06/10/15.
//

#ifndef INCLUDED_AERON_DRIVER_NETWORKINTERFACE__
#define INCLUDED_AERON_DRIVER_NETWORKINTERFACE__


#include <memory>
#include "InetAddress.h"

namespace aeron { namespace driver { namespace media {

class NetworkInterface
{
public:
    NetworkInterface(std::unique_ptr<InetAddress> address, const char* name, unsigned int index)
        : m_address(std::move(address)), m_name(name), m_index(index)
    {}

    inline InetAddress& address() const
    {
        return *m_address;
    }

    inline const char* name() const
    {
        return m_name;
    }

    inline unsigned int index() const
    {
        return m_index;
    }

    void setAsMulticastInterface(int socketFd) const;

private:
    std::unique_ptr<InetAddress> m_address;
    const char* m_name;
    unsigned int m_index;
};

}}}

#endif //AERON_NETWORKINTERFACE_H
