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

    friend bool operator==(const InetAddress& a, const InetAddress& b);
    friend bool operator!=(const InetAddress& a, const InetAddress& b);

    static std::unique_ptr<InetAddress> parse(const char* address);
    static std::unique_ptr<InetAddress> parse(std::string const & address);

    static std::unique_ptr<InetAddress> fromIPv4(std::string &address, uint16_t port);
    static std::unique_ptr<InetAddress> fromIPv4(const char* address, uint16_t port)
    {
        std::string s{address};
        return fromIPv4(s, port);
    }

    static std::unique_ptr<InetAddress> fromIPv6(std::string &address, uint16_t port);
    static std::unique_ptr<InetAddress> fromIPv6(const char* address, uint16_t port)
    {
        std::string s{address};
        return fromIPv6(s, port);
    }

private:
    sockaddr* m_address;
    socklen_t m_length;
    int m_domain;
    int m_type;
    int m_protocol;

};

inline bool operator==(const InetAddress& a, const InetAddress& b)
{
    return a.m_length == b.m_length && memcmp(a.m_address, b.m_address, a.m_length) == 0;
}

inline bool operator!=(const InetAddress& a, const InetAddress& b)
{
    return a.m_length == b.m_length && memcmp(a.m_address, b.m_address, a.m_length);
}

}}};

#endif //AERON_SOCKETADDRESS_H
