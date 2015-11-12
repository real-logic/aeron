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

#include "UdpChannel.h"
#include "uri/AeronUri.h"
#include "media/InetAddress.h"
#include "media/InterfaceSearchAddress.h"

using namespace aeron::driver::media;
using namespace aeron::driver::uri;

static const char* GROUP_KEY = "group";
static const char* INTERFACE_KEY = "interface";
static const char* LOCAL_KEY = "local";
static const char* REMOTE_KEY = "remote";

static bool isMulticastUri(const AeronUri* uri)
{
    return uri->hasParam("group");
}

static void validateUri(const AeronUri* uri)
{
    if (uri->media() != "udp")
    {
        throw InvalidChannelException("Only UDP media supported for UdpChannel", SOURCEINFO);
    }

    bool hasMulticastKeys = uri->hasParam(GROUP_KEY) || uri->hasParam(INTERFACE_KEY);
    bool hasUnicastKeys = uri->hasParam(LOCAL_KEY) || uri->hasParam(REMOTE_KEY);

    if (!(hasMulticastKeys ^ hasUnicastKeys))
    {
        throw InvalidChannelException("May only specific unicast or multicast configuration, not both", SOURCEINFO);
    }
}

std::unique_ptr<UdpChannel> UdpChannel::parse(const char* uri, int familyHint, InterfaceLookup& lookup)
{
    std::string uriStr{uri};

    auto aeronUri = AeronUri::parse(uriStr);

    validateUri(aeronUri);

    if (isMulticastUri(aeronUri))
    {
        auto dataAddress = InetAddress::parse(aeronUri->param(GROUP_KEY));

        if (dataAddress->isEven())
        {
            throw InvalidChannelException("Multicast data addresses must be odd", SOURCEINFO);
        }

        std::string wildcardAddress{"0.0.0.0/0"};
        auto controlAddress = dataAddress->nextAddress();
        auto interfaceAddressString = aeronUri->param(INTERFACE_KEY, wildcardAddress);
        auto interfaceSearchAddress = InterfaceSearchAddress::parse(interfaceAddressString, familyHint);
        auto localAddress = interfaceSearchAddress->findLocalAddress(lookup);

        return std::unique_ptr<UdpChannel>{new UdpChannel{dataAddress, controlAddress, localAddress, true}};
    }
    else
    {
        auto remoteAddress = InetAddress::parse(aeronUri->param(REMOTE_KEY));
        std::unique_ptr<InetAddress> localAddress = (aeronUri->hasParam(LOCAL_KEY))
            ? InetAddress::parse(aeronUri->param(LOCAL_KEY))
            : InetAddress::any(familyHint);

        std::unique_ptr<InetAddress> empty{nullptr};
        auto localInterface = std::unique_ptr<NetworkInterface>{new NetworkInterface{std::move(localAddress), nullptr, 0}};
        return std::unique_ptr<UdpChannel>(new UdpChannel{remoteAddress, empty, localInterface, false});
    }
}

const char* UdpChannel::canonicalForm()
{
    return nullptr;
}
