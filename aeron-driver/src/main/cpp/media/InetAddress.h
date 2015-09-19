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

#ifndef INCLUDE_AERON_DRIVER_MEDIA_INET_ADDRESS_
#define INCLUDE_AERON_DRIVER_MEDIA_INET_ADDRESS_

#include <string>
#include <memory>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "util/Exceptions.h"

namespace aeron { namespace driver { namespace media {

class InetAddress
{
public:
    virtual sockaddr* address() const = 0;
    virtual socklen_t length() const = 0;
    virtual int domain() const = 0;

    int type() const
    {
        return SOCK_DGRAM;
    }

    int protocol() const
    {
        return IPPROTO_UDP;
    }

    virtual void output(std::ostream& os) const = 0;
    virtual uint16_t port() const = 0;
    virtual bool isEven() const = 0;
    virtual bool equals(const InetAddress& other) const = 0;
    virtual std::unique_ptr<InetAddress> nextAddress() const = 0;
    virtual bool matches(const InetAddress& candidate, std::uint32_t subnetPrefix) const = 0;

    static std::unique_ptr<InetAddress> parse(const char* address, int familyHint = PF_INET);
    static std::unique_ptr<InetAddress> parse(std::string const & address, int familyHint = PF_INET);

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

    static std::unique_ptr<InetAddress> fromHostname(std::string& address, uint16_t port, int familyHint);
    static std::unique_ptr<InetAddress> any(int familyHint);
};

class Inet4Address : public InetAddress
{
public:
    Inet4Address(in_addr address, uint16_t port)
    {
        m_socketAddress.sin_family = AF_INET;
        m_socketAddress.sin_addr = address;
        m_socketAddress.sin_port = htons(port);
    }

    Inet4Address(const char* addrStr, uint16_t port)
    {
        if (!inet_pton(AF_INET, addrStr, &m_socketAddress.sin_addr))
        {
            throw aeron::util::IOException("Failed to parse IPv4 address", SOURCEINFO);
        }

        m_socketAddress.sin_family = AF_INET;
        m_socketAddress.sin_port = htons(port);
    }

    sockaddr* address() const
    {
        return (sockaddr*) &m_socketAddress;
    }

    socklen_t length() const
    {
        return sizeof(sockaddr_in);
    }

    int domain() const
    {
        return PF_INET;
    }

    uint16_t port() const
    {
        return ntohs(m_socketAddress.sin_port);
    }

    in_addr addr()
    {
        return m_socketAddress.sin_addr;
    }

    bool isEven() const;
    bool equals(const InetAddress& other) const;
    void output(std::ostream& os) const;
    std::unique_ptr<InetAddress> nextAddress() const;
    bool matches(const InetAddress& candidate, std::uint32_t subnetPrefix) const;

private:
    sockaddr_in m_socketAddress;
};

class Inet6Address : public InetAddress
{
public:
    Inet6Address(in6_addr address, uint16_t port)
    {
        m_socketAddress.sin6_family = AF_INET6;
        m_socketAddress.sin6_addr = address;
        m_socketAddress.sin6_port = htons(port);
    }

    Inet6Address(const char* address, uint16_t port)
    {
        if (!inet_pton(AF_INET6, address, &m_socketAddress.sin6_addr))
        {
            throw aeron::util::IOException("Failed to parse IPv6 address", SOURCEINFO);
        }

        m_socketAddress.sin6_family = AF_INET6;
        m_socketAddress.sin6_port = htons(port);
    }

    sockaddr* address() const
    {
        return (sockaddr*) &m_socketAddress;
    }

    socklen_t length() const
    {
        return sizeof(sockaddr_in);
    }

    int domain() const
    {
        return PF_INET6;
    }

    uint16_t port() const
    {
        return ntohs(m_socketAddress.sin6_port);
    }

    in6_addr addr()
    {
        return m_socketAddress.sin6_addr;
    }

    bool isEven() const;
    bool equals(const InetAddress& other) const;
    void output(std::ostream& os) const;
    std::unique_ptr<InetAddress> nextAddress() const;
    bool matches(const InetAddress& candidate, std::uint32_t subnetPrefix) const;

private:
    sockaddr_in6 m_socketAddress;
};

inline bool operator==(const InetAddress& a, const InetAddress& b)
{
    return a.domain() == b.domain() && a.equals(b);
}

inline bool operator!=(const InetAddress& a, const InetAddress& b)
{
    return !(a == b);
}

inline std::ostream& operator<<(std::ostream& os, const InetAddress& dt)
{
    dt.output(os);
    return os;
}

}}};

#endif //AERON_SOCKETADDRESS_H
