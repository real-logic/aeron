//
// Created by Michael Barker on 03/09/15.
//

#include <cstdlib>
#include <cinttypes>
#include <string>
#include <regex>
#include <arpa/inet.h>
#include <netdb.h>
#include <iostream>

#include "InetAddress.h"
#include "uri/NetUtil.h"
#include "util/Exceptions.h"

using namespace aeron::driver::media;

InetAddress::InetAddress(sockaddr* address, socklen_t length, int domain, int type, int protocol) :
    m_address(address), m_length(length), m_domain(domain), m_type(type), m_protocol(protocol)
{
}

InetAddress::~InetAddress()
{
//    std::cout << "delete" << '\n';
//    delete m_address;
}

struct InfoDeleter
{
    InfoDeleter() {};
    InfoDeleter(const InfoDeleter&) {}
    InfoDeleter(InfoDeleter&) {}

    void operator()(addrinfo* addr) const
    {
        freeaddrinfo(addr);
    }
};

std::unique_ptr<InetAddress> InetAddress::fromIPv4(std::string& address, uint16_t port)
{
    struct in_addr addr;

    if (!inet_pton(AF_INET, address.c_str(), &addr))
    {
        throw aeron::util::IOException("Failed to parse IPv4 address", SOURCEINFO);
    }

    struct sockaddr_in* sockaddr_in = new struct sockaddr_in;
    sockaddr_in->sin_addr = addr;
    sockaddr_in->sin_port = htons(port);
    sockaddr_in->sin_family = AF_INET;

    sockaddr* sockaddr1 = (struct sockaddr*) sockaddr_in;

    return std::unique_ptr<InetAddress>{new InetAddress{sockaddr1, sizeof(sockaddr_in), PF_INET, SOCK_DGRAM, 0}};
}

std::unique_ptr<InetAddress> InetAddress::fromIPv6(std::string& address, uint16_t port)
{
    struct in6_addr addr;

    if (!inet_pton(AF_INET6, address.c_str(), &addr))
    {
        throw aeron::util::IOException("Failed to parse IPv4 address", SOURCEINFO);
    }

    sockaddr_in6* socket_addr = new sockaddr_in6;
    socket_addr->sin6_addr = addr;
    socket_addr->sin6_port = htons(port);
    socket_addr->sin6_family = AF_INET;

    sockaddr* sockaddr1 = (sockaddr*) socket_addr;

    return std::unique_ptr<InetAddress>{new InetAddress{sockaddr1, sizeof(sockaddr_in6), PF_INET6, SOCK_DGRAM, 0}};
}

std::unique_ptr<InetAddress> InetAddress::parse(const char* addressString)
{
    std::string s{addressString};
    return parse(s);
}

std::unique_ptr<InetAddress> InetAddress::parse(std::string const & addressString)
{
    std::regex ipV4{"([^:]+)(?::([0-9]+))?"};
    std::regex ipv6{"\\[([0-9A-Fa-f:]+)(?:%([a-zA-Z0-9_.~-]+))?\\](?::([0-9]+))?"};
    std::smatch results;

    if (std::regex_match(addressString, results, ipv6))
    {
        auto inetAddressStr = results[1].str();
        auto scope = results[2].str();
        auto portStr = results[3].str();
        auto port = atoi(portStr.c_str());

        return fromIPv6(inetAddressStr, port);
    }

    if (std::regex_match(addressString, results, ipV4) && results.size() == 3)
    {
        auto inetAddressStr = results[1].str();
        auto portStr = results[2].str();
        auto port1 = atoi(portStr.c_str());

        return fromIPv4(inetAddressStr, port1);
    }

    throw aeron::util::IOException("Address does not match IPv4 or IPv6 string", SOURCEINFO);
}


bool InetAddress::isEven() const
{
    if (domain() == PF_INET)
    {
        sockaddr_in* ipv4_address = (sockaddr_in*) address();
        return aeron::driver::uri::NetUtil::isEven(ipv4_address->sin_addr);
    }
    else if (domain() == PF_INET6)
    {
        sockaddr_in6* ipv6_address = (sockaddr_in6*) address();
        return aeron::driver::uri::NetUtil::isEven(ipv6_address->sin6_addr);
    }

    return false;
}

uint16_t InetAddress::port() const
{
    if (domain() == PF_INET)
    {
        sockaddr_in* ipv4_address = (sockaddr_in*) address();
        return ntohs(ipv4_address->sin_port);
    }
    else if (domain() == PF_INET6)
    {
        sockaddr_in6* ipv6_address = (sockaddr_in6*) address();
        return ntohs(ipv6_address->sin6_port);
    }

    return 0;
}
