//
// Created by Michael Barker on 21/09/15.
//

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERMREBUILDER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERMREBUILDER__

#include "FrameDescriptor.h"

namespace aeron { namespace concurrent { namespace logbuffer
{

namespace TermRebuilder {

inline void insert(AtomicBuffer& termBuffer, std::int32_t termOffset, AtomicBuffer& packet, std::int32_t length)
{
/*
        final int firstFrameLength = packet.getInt(0, LITTLE_ENDIAN);
        packet.putIntOrdered(0, 0);

        termBuffer.putBytes(termOffset, packet, 0, length);
        frameLengthOrdered(termBuffer, termOffset, firstFrameLength);
 */
    const std::int32_t firstFrameLength = packet.getInt32(0);
    packet.putInt32Ordered(0, 0);

    termBuffer.putBytes(termOffset, packet, 0, length);
    FrameDescriptor::frameLengthOrdered(termBuffer, termOffset, firstFrameLength);

    return;
}

};

}}};
#endif //AERON_TERMREBUILDER_H
