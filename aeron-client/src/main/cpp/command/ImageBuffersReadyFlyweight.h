/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
#ifndef INCLUDED_AERON_COMMAND_CONNECTIONREADYFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_CONNECTIONREADYFLYWEIGHT__

#include <cstdint>
#include <stddef.h>
#include <util/Exceptions.h>
#include <util/StringUtil.h>
#include "Flyweight.h"

namespace aeron { namespace command {

/**
* Message to denote that new buffers have been added for a subscription.
*
* NOTE: Layout should be SBE compliant
*
* @see ControlProtocolEvents
*
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Session ID                           |
* +---------------------------------------------------------------+
* |                           Stream ID                           |
* +---------------------------------------------------------------+
* |                Subscriber Position Block Length               |
* +---------------------------------------------------------------+
* |                   Subscriber Position Count                   |
* +---------------------------------------------------------------+
* |                      Subscriber Position Id 0                 |
* +---------------------------------------------------------------+
* |                         Registration Id 0                     |
* |                                                               |
* +---------------------------------------------------------------+
* |                     Subscriber Position Id 1                  |
* +---------------------------------------------------------------+
* |                         Registration Id 1                     |
* |                                                               |
* +---------------------------------------------------------------+
* |                                                              ...
*...     Up to "Position Indicators Count" entries of this form
* +---------------------------------------------------------------+
* |                         Log File Length                       |
* +---------------------------------------------------------------+
* |                          Log File Name                       ...
*...                                                              |
* +---------------------------------------------------------------+
* |                     Source identity Length                    |
* +---------------------------------------------------------------+
* |                     Source identity Name                     ...
*...                                                              |
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct ImageBuffersReadyDefn
{
    std::int64_t correlationId;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t subscriberPositionBlockLength;
    std::int32_t subscriberPositionCount;

    struct SubscriberPosition
    {
        std::int32_t indicatorId;
        std::int64_t registrationId;
    };
};
#pragma pack(pop)

class ImageBuffersReadyFlyweight : public Flyweight<ImageBuffersReadyDefn>
{
public:
    typedef ImageBuffersReadyFlyweight this_t;

    inline ImageBuffersReadyFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset)
        : Flyweight<ImageBuffersReadyDefn>(buffer, offset)
    {
    }

    inline std::int64_t correlationId() const
    {
        return m_struct.correlationId;
    }

    inline this_t& correlationId(std::int64_t value)
    {
        m_struct.correlationId = value;
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

    inline std::int32_t subscriberPositionCount() const
    {
        return m_struct.subscriberPositionCount;
    }

    inline this_t& subscriberPositionCount(std::int32_t value)
    {
        m_struct.subscriberPositionBlockLength = sizeof(ImageBuffersReadyDefn::SubscriberPosition);
        m_struct.subscriberPositionCount = value;
        return *this;
    }

    inline this_t& subscriberPosition(std::int32_t index, const ImageBuffersReadyDefn::SubscriberPosition& value)
    {
        overlayStruct<ImageBuffersReadyDefn::SubscriberPosition>(subscriberPositionOffset(index)) = value;
        return *this;
    }

    inline const ImageBuffersReadyDefn::SubscriberPosition subscriberPosition(std::int32_t index) const
    {
        return overlayStruct<ImageBuffersReadyDefn::SubscriberPosition>(subscriberPositionOffset(index));
    }

    inline const ImageBuffersReadyDefn::SubscriberPosition* subscriberPositions() const
    {
        return &overlayStruct<ImageBuffersReadyDefn::SubscriberPosition>(subscriberPositionOffset(0));
    }

    inline std::string logFileName() const
    {
        return stringGet(logFileNameOffset());
    }

    inline this_t& logFileName(const std::string& value)
    {
        stringPut(logFileNameOffset(), value);
        return *this;
    }

    inline std::string sourceIdentity() const
    {
        return stringGet(sourceIdentityOffset());
    }

    inline this_t& sourceIdentity(const std::string &value)
    {
        stringPut(sourceIdentityOffset(), value);
        return *this;
    }

    inline std::int32_t length()
    {
        const util::index_t startOfSourceIdentity = sourceIdentityOffset();

        return startOfSourceIdentity + stringGetLength(startOfSourceIdentity) + (util::index_t)sizeof(std::int32_t);
    }

private:

    inline util::index_t subscriberPositionOffset(int index) const
    {
        const util::index_t startOfPositions = sizeof(ImageBuffersReadyDefn);

        return startOfPositions + (index * (util::index_t)sizeof(ImageBuffersReadyDefn::SubscriberPosition));
    }

    inline util::index_t logFileNameOffset() const
    {
        return subscriberPositionOffset(subscriberPositionCount());
    }

    inline util::index_t sourceIdentityOffset() const
    {
        const util::index_t startOfLogFileName = logFileNameOffset();
        return startOfLogFileName + stringGetLength(startOfLogFileName) + (util::index_t)sizeof(std::int32_t);
    }
};

}};

#endif