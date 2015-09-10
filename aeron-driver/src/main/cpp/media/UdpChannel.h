//
// Created by Michael Barker on 26/08/15.
//

#ifndef INCLUDED_AERON_DRIVER_UDP_CHANNEL_H_
#define INCLUDED_AERON_DRIVER_UDP_CHANNEL_H_

#include <memory>
#include "util/Exceptions.h"
#include "InetAddress.h"

namespace aeron { namespace driver { namespace media {

DECLARE_SOURCED_EXCEPTION (InvalidChannelException);

class UdpChannel
{
public:
    UdpChannel(std::unique_ptr<InetAddress>& remoteData, std::unique_ptr<InetAddress>& remoteControl)
        : m_remoteControl(std::move(remoteControl)), m_remoteData(std::move(remoteData))
    {
    }

    const char* canonicalForm();

    InetAddress& remoteControl() const
    {
        return *m_remoteControl;
    }

    InetAddress& remoteData() const
    {
        return *m_remoteData;
    }

    static std::unique_ptr<UdpChannel> parse(const char* uri);

private:
    std::unique_ptr<InetAddress> m_remoteControl;
    std::unique_ptr<InetAddress> m_remoteData;
};

}}}

#endif //AERON_UDPCHANNEL_H
