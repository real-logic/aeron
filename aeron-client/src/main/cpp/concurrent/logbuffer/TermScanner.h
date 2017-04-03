//
// Created by Michael Barker on 25/09/15.
//

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERMSCANNER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERMSCANNER__

#include <util/BitUtil.h>
#include "FrameDescriptor.h"

namespace aeron { namespace concurrent { namespace logbuffer
{

namespace TermScanner
{

inline std::int64_t scanOutcome(std::int32_t padding, std::int32_t available)
{
    return ((std::int64_t) padding) << 32 | available;
}

inline std::int32_t available(std::int64_t scanOutcome)
{
    return (std::int32_t) scanOutcome;
}

inline std::int32_t padding(std::int64_t scanOutcome)
{
    return (std::int32_t) (scanOutcome >> 32);
}

inline std::int64_t scanForAvailability(AtomicBuffer& termBuffer, std::int32_t offset, std::int32_t maxLength)
{
//    maxLength = Math.min(maxLength, termBuffer.capacity() - offset);
//    int available = 0;
//    int padding = 0;
    maxLength = std::min(maxLength, termBuffer.capacity() - offset);
    std::int32_t available = 0;
    std::int32_t padding = 0;

//    do
//    {
//        final int frameOffset = offset + available;
//        final int frameLength = frameLengthVolatile(termBuffer, frameOffset);
//        if (frameLength <= 0)
//        {
//            break;
//        }
//
//        int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
//        if (isPaddingFrame(termBuffer, frameOffset))
//        {
//            padding = alignedFrameLength - HEADER_LENGTH;
//            alignedFrameLength = HEADER_LENGTH;
//        }
//
//        available += alignedFrameLength;
//
//        if (available > maxLength)
//        {
//            available -= alignedFrameLength;
//            padding = 0;
//            break;
//        }
//    }
//    while ((available + padding) < maxLength);
//
//    return scanOutcome(padding, available);

    do
    {
        const util::index_t frameOffset = offset + available;
        const util::index_t frameLength = FrameDescriptor::frameLengthVolatile(termBuffer, frameOffset);
        if (frameLength <= 0)
        {
            break;
        }

        util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        if (FrameDescriptor::isPaddingFrame(termBuffer, frameOffset))
        {
            padding = alignedFrameLength - DataFrameHeader::LENGTH;
            alignedFrameLength = DataFrameHeader::LENGTH;
        }

        available += alignedFrameLength;

        if (available > maxLength)
        {
            available -= alignedFrameLength;
            padding = 0;
            break;
        }
    }
    while ((available + padding) < maxLength);

    return scanOutcome(padding, available);
}

}

}}}

#endif //AERON_TERMSCANNER_H
