//
// Created by Michael Barker on 11/09/15.
//

#include <regex>
#include <iostream>
#include <string>
#include <sstream>

#include "util/StringUtil.h"
#include "util/Exceptions.h"
#include "InterfaceSearchAddress.h"

using namespace aeron::driver::media;

static std::uint32_t parseWildcard(std::string&& s, std::uint32_t defaultVal)
{
    return s.size() > 0 ? aeron::util::fromString<std::uint32_t>(s) : defaultVal;
}

std::unique_ptr<InterfaceSearchAddress> InterfaceSearchAddress::parse(std::string &str)
{
    std::regex ipV6{"\\[([0-9A-Fa-f:]+)(?:%([a-zA-Z0-9_.~-]+))?\\](?::([0-9]+))?(?:/([0-9]+))?"};
    std::regex ipV4{"([^:/]+)(?::([0-9]+))?(?:/([0-9]+))?"};
    std::smatch results;

    if (std::regex_match(str, results, ipV6))
    {
        auto inetAddressStr = results[1].str();
        auto scope = results[2].str();
        auto port = aeron::util::fromString<std::uint16_t>(results[3].str());
        auto wildcard = parseWildcard(results[4].str(), 128);
        auto inetAddress = InetAddress::fromIPv6(inetAddressStr, port);

        return std::unique_ptr<InterfaceSearchAddress>{new InterfaceSearchAddress{inetAddress, wildcard}};
    }
    if (std::regex_match(str, results, ipV4))
    {
        auto inetAddressStr = results[1].str();
        auto port = aeron::util::fromString<std::uint16_t>(results[2].str());
        auto wildcard = parseWildcard(results[3].str(), 32);
        auto inetAddress = InetAddress::fromIPv4(inetAddressStr, port);

        return std::unique_ptr<InterfaceSearchAddress>{new InterfaceSearchAddress{inetAddress, wildcard}};
    }

    throw aeron::util::ParseException{"Must be valid address", SOURCEINFO};
}

bool InterfaceSearchAddress::matches(const InetAddress &candidate) const
{
    return m_inetAddress->domain() == candidate.domain() && m_inetAddress->matches(candidate, m_subnetPrefix);
}
