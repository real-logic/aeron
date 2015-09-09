//
// Created by Michael Barker on 26/08/15.
//

#ifndef INCLUDED_AERON_DRIVER_UDP_CHANNEL_
#define INCLUDED_AERON_DRIVER_UDP_CHANNEL_

#include <memory>
#include "util/Exceptions.h"

namespace aeron { namespace driver { namespace media {

DECLARE_SOURCED_EXCEPTION (InvalidChannelException);

class UdpChannel
{
public:
    const char* canonicalForm();

    static std::unique_ptr<UdpChannel> parse(const char* uri);
};

}}}

#endif //AERON_UDPCHANNEL_H
