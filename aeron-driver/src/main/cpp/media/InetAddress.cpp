//
// Created by Michael Barker on 03/09/15.
//

#include <cstdlib>
#include <cinttypes>
#include <string>
#include <regex>
#include <arpa/inet.h>
#include <netdb.h>

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
    delete m_address;
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

std::unique_ptr<InetAddress> InetAddress::fromIpString(std::string& address, uint16_t port)
{
    return fromIpString(address.c_str(), port);
}

std::unique_ptr<InetAddress> InetAddress::fromIpString(const char* address, uint16_t port)
{
    addrinfo* info = NULL;

    if (getaddrinfo(address, NULL, NULL, &info))
    {
        throw aeron::util::IOException("Failed to get address info", SOURCEINFO);
    }

    InfoDeleter d;
    std::unique_ptr<addrinfo, InfoDeleter> ptr(info, d);
    sockaddr* sockaddr;
    socklen_t sockaddr_len;

    if (info->ai_family == AF_INET6)
    {
        sockaddr_in6* addr = new sockaddr_in6;
        sockaddr = (struct sockaddr*) (addr);
        sockaddr_len = sizeof(sockaddr_in6);
        memcpy(sockaddr, ptr->ai_addr, sockaddr_len);
        addr->sin6_port = htons(port);
    }
    else if (info->ai_family == AF_INET)
    {
        sockaddr_in* addr = new sockaddr_in;
        sockaddr = (struct sockaddr*) (addr);
        sockaddr_len = sizeof(sockaddr_in6);
        memcpy(sockaddr, ptr->ai_addr, sockaddr_len);
        addr->sin_port = htons(port);
    }
    else
    {
        throw aeron::util::IOException("Only AF_INET and AF_INET6 addresses are supported", SOURCEINFO);
    }


    return std::unique_ptr<InetAddress>{new InetAddress{sockaddr, sockaddr_len, ptr->ai_family, ptr->ai_protocol, 0}};
}

static std::unique_ptr<InetAddress> loadFromMatch(std::smatch const & matchResults)
{
    auto inetAddressStr = matchResults[1].str();
    auto portStr = matchResults[2].str();
    auto port = atoi(portStr.c_str());

    return InetAddress::fromIpString(inetAddressStr, port);
}

std::unique_ptr<InetAddress> InetAddress::parse(const char* addressString)
{
    std::string s{addressString};
    return parse(s);
}

std::unique_ptr<InetAddress> InetAddress::parse(std::string const & addressString)
{
    std::regex ipV4{"([^:]+)(?::([0-9]+))?"};
    std::regex ipv6{"\\[([0-9A-Fa-f:]+)(?:%[a-zA-Z0-9_.~-]+)?\\](?::([0-9]+))?"};
    std::smatch results;

    if (std::regex_match(addressString, results, ipV4) && results.size() == 3)
    {
        return loadFromMatch(results);
    }

    if (std::regex_match(addressString, results, ipv6) && results.size() == 3)
    {
        return loadFromMatch(results);
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
