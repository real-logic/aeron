/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_COMMAND_STATUSMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_STATUSMESSAGEFLYWEIGHT__

#include <cstdint>
#include <string>
#include <stddef.h>
#include <command/Flyweight.h>
#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>
#include "HeaderFlyweight.h"

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
    HeaderDefn header;
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

class StatusMessageFlyweight : public HeaderFlyweight
{
public:
    typedef StatusMessageFlyweight this_t;

    inline StatusMessageFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset)
        : HeaderFlyweight(buffer, offset), m_struct(overlayStruct<StatusMessageDefn>(0))
    {
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

    inline static constexpr std::int32_t headerLength()
    {
        return sizeof(StatusMessageDefn);
    }

private:
    StatusMessageDefn& m_struct;
};

}}

#endif //AERON_STATUSMESSAGEFLYWEIGHT_H
