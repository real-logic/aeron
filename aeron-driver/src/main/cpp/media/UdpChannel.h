//
// Created by Michael Barker on 26/08/15.
//

#ifndef INCLUDED_AERON_DRIVER_UDP_CHANNEL_
#define INCLUDED_AERON_DRIVER_UDP_CHANNEL_

#include <memory>

namespace aeron { namespace driver { namespace media {

class UdpChannel
{
public:
    const char* canonicalForm();

    static std::unique_ptr<UdpChannel> parse(const char* uri);
};

}}}

#endif //AERON_UDPCHANNEL_H
