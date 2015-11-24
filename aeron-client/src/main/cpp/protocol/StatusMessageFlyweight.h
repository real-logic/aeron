//
// Created by Michael Barker on 24/11/2015.
//

#ifndef INCLUDED_AERON_COMMAND_STATUSMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_STATUSMESSAGEFLYWEIGHT__

#include <cstdint>
#include <string>
#include <stddef.h>
#include <command/Flyweight.h>
#include "../concurrent/AtomicBuffer.h"
#include "../util/Index.h"

namespace aeron { namespace protocol {


/**
 * Flow/Congestion control message to send feedback from subscriptions to publications.
 *
 * <p>
 *    0                   1                   2                   3
 *    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   |R|                 Frame Length (=header + data)               |
 *   +---------------+-+-------------+-------------------------------+
 *   |   Version     |S|    Flags    |          Type (=0x03)         |
 *   +---------------+-+-------------+-------------------------------+
 *   |                          Session ID                           |
 *   +---------------------------------------------------------------+
 *   |                           Stream ID                           |
 *   +---------------------------------------------------------------+
 *   |                      Consumption Term ID                      |
 *   +---------------------------------------------------------------+
 *   |R|                  Consumption Term Offset                    |
 *   +---------------------------------------------------------------+
 *   |                        Receiver Window                        |
 *   +---------------------------------------------------------------+
 *   |                  Application Specific Feedback               ...
 *  ...                                                              |
 *   +---------------------------------------------------------------+
 */

#pragma pack(push)
#pragma pack(4)
struct StatusMessageDefn
{
    std::int32_t frameLength;
    std::int8_t version;
    std::int8_t flags;
    std::int16_t type;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t consumptionTermId;
    std::int32_t consumptionTermOffset;
    std::int32_t receiverWindow;

    // TODO: Lets not support this for now.
//    struct
//    {
//        std::int32_t applicationSpecificFeedbackLength;
//        std::uint8_t applicationSpecificFeedbackData[1];
//    } applicationSpecificFeedback;
};
#pragma pack(pop)

class StatusMessageFlyweight : command::Flyweight<StatusMessageDefn>
{
    typedef StatusMessageFlyweight this_t;

    inline StatusMessageFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset)
        : command::Flyweight<StatusMessageDefn>(buffer, offset)
    {
    }

    inline std::int32_t frameLength() const
    {
        return m_struct.frameLength;
    }

    inline this_t& frameLength(std::int32_t value)
    {
        m_struct.frameLength = value;
        return *this;
    }

    inline std::int8_t version() const
    {
        return m_struct.version;
    }

    inline this_t& version(std::int8_t value)
    {
        m_struct.version = value;
        return *this;
    }

    inline std::int8_t flags() const
    {
        return m_struct.flags;
    }

    inline this_t& flags(std::int8_t value)
    {
        m_struct.flags = value;
        return *this;
    }

    inline std::int16_t type() const
    {
        return m_struct.type;
    }

    inline this_t& type(std::int16_t value)
    {
        m_struct.type = value;
        return *this;
    }

    inline std::int32_t sessionId() const
    {
        return m_struct.sessionId;
    }

    inline this_t& sessionId(std::int32_t value)
    {
        m_struct.sessionId = value;
        return *this;
    }

    inline std::int32_t streamId() const
    {
        return m_struct.streamId;
    }

    inline this_t& streamId(std::int32_t value)
    {
        m_struct.streamId = value;
        return *this;
    }

    inline std::int32_t consumptionTermId() const
    {
        return m_struct.consumptionTermId;
    }

    inline this_t& consumptionTermId(std::int32_t value)
    {
        m_struct.consumptionTermId = value;
        return *this;
    }

    inline std::int32_t consumptionTermOffset() const
    {
        return m_struct.consumptionTermOffset;
    }

    inline this_t& consumptionTermOffset(std::int32_t value)
    {
        m_struct.consumptionTermOffset = value;
        return *this;
    }

    inline std::int32_t receiverWindow() const
    {
        return m_struct.receiverWindow;
    }

    inline this_t& receiverWindow(std::int32_t value)
    {
        m_struct.receiverWindow = value;
        return *this;
    }

    inline static std::int32_t length()
    {
        return sizeof(StatusMessageDefn);
    }
};

}}

#endif //AERON_STATUSMESSAGEFLYWEIGHT_H
