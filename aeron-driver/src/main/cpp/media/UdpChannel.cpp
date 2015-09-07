//
// Created by Michael Barker on 26/08/15.
//

#include "UdpChannel.h"
#include "uri/AeronUri.h"
#include "media/InetAddress.h"

using namespace aeron::driver::media;
using namespace aeron::driver::uri;

static bool isMulticast(const AeronUri* uri)
{
    return uri->hasParam("group");
}

std::unique_ptr<UdpChannel> UdpChannel::parse(const char* uri)
{
    std::string uriStr{uri};

    auto aeronUri = AeronUri::parse(uriStr);

    if (isMulticast(aeronUri))
    {
        auto dataAddressStr = aeronUri->param("group");
        auto dataAddress = InetAddress::parse(dataAddressStr);
    }
    else
    {

    }

    return std::unique_ptr<UdpChannel>(nullptr);
}

const char* UdpChannel::canonicalForm()
{
    return nullptr;
}
