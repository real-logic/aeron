//
// Created by Michael Barker on 24/11/2015.
//

#ifndef INCLUDED_AERON_DRIVER_MEDIA_RECEIVECHANNELENDPOINT__
#define INCLUDED_AERON_DRIVER_MEDIA_RECEIVECHANNELENDPOINT__

#include <protocol/StatusMessageFlyweight.h>
#include <media/UdpChannelTransport.h>

namespace aeron { namespace driver { namespace media {

class ReceiveChannelEndpoint : public UdpChannelTransport
{
public:
    inline ReceiveChannelEndpoint(
        std::unique_ptr<UdpChannel>& channel)
        : UdpChannelTransport(channel, &channel->remoteData(), &channel->remoteData(), nullptr)
    {
    }
};

}}}

#endif //INCLUDED_AERON_DRIVER_MEDIA_RECEIVECHANNELENDPOINT__
