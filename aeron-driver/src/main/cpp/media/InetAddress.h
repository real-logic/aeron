//
// Created by Michael Barker on 03/09/15.
//

#ifndef INCLUDE_AERON_DRIVER_MEDIA_INET_ADDRESS_
#define INCLUDE_AERON_DRIVER_MEDIA_INET_ADDRESS_

#include <string>
#include <memory>
#include <sys/types.h>
#include <sys/socket.h>

namespace aeron { namespace driver { namespace media {

class InetAddress
{
public:
    InetAddress(sockaddr* address, socklen_t length, int domain, int type, int protocol);
    ~InetAddress();

    sockaddr* address() const
    {
        return m_address;
    }

    socklen_t length() const
    {
        return m_length;
    }

    int domain() const
    {
        return m_domain;
    }

    int type() const
    {
        return m_type;
    }

    int protocol() const
    {
        return m_protocol;
    }

    uint16_t port() const;

    bool isEven() const;

    static std::unique_ptr<InetAddress> fromIpString(const char* address, uint16_t port = 0);
    static std::unique_ptr<InetAddress> fromIpString(std::string& address, uint16_t port = 0);
    static std::unique_ptr<InetAddress> parse(const char* address);
    static std::unique_ptr<InetAddress> parse(std::string const & address);

private:
    sockaddr* m_address;
    socklen_t m_length;
    int m_domain;
    int m_type;
    int m_protocol;
};

}}};

#endif //AERON_SOCKETADDRESS_H
