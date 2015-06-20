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
* @see ControlProtocolEvents
*
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
* |                        Joining Position                       |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Session ID                           |
* +---------------------------------------------------------------+
* |                           Stream ID                           |
* +---------------------------------------------------------------+
* |                   Subscriber Position Count                   |
* +---------------------------------------------------------------+
* |                         Log File Length                       |
* +---------------------------------------------------------------+
* |                          Log File Name                      ...
* ...                                                             |
* +---------------------------------------------------------------+
* |                     Source Identity Length                    |
* +---------------------------------------------------------------+
* |                      Source Identity Name                    ...
* ...                                                             |
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
* |                                                             ...
* Up to "Position Indicators Count" entries of this form
*/

#pragma pack(push)
#pragma pack(4)
struct ConnectionBuffersReadyDefn
{
    std::int64_t correlationId;
    std::int64_t joiningPosition;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t subscriberPositionCount;
    struct
    {
        std::int32_t logFileLength;
        std::int8_t logFileData[1];
    } logFile;

    struct SubscriberPosition
    {
        std::int32_t indicatorId;
        std::int64_t registrationId;
    };
};
#pragma pack(pop)

class ConnectionBuffersReadyFlyweight : public Flyweight<ConnectionBuffersReadyDefn>
{
public:
    typedef ConnectionBuffersReadyFlyweight this_t;

    inline ConnectionBuffersReadyFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset)
        : Flyweight<ConnectionBuffersReadyDefn>(buffer, offset)
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

    inline std::int64_t joiningPosition() const
    {
        return m_struct.joiningPosition;
    }

    inline this_t& joiningPosition(std::int64_t value)
    {
        m_struct.joiningPosition = value;
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
        m_struct.subscriberPositionCount = value;
        return *this;
    }

    inline std::string logFileName() const
    {
        return stringGet(offsetof(ConnectionBuffersReadyDefn, logFile));
    }

    inline this_t& logFileName(const std::string& value)
    {
        stringPut(offsetof(ConnectionBuffersReadyDefn, logFile), value);
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

    inline this_t& subscriberPosition(std::int32_t index, const ConnectionBuffersReadyDefn::SubscriberPosition& value)
    {
        overlayStruct<ConnectionBuffersReadyDefn::SubscriberPosition>(subscriberPositionOffset(index)) = value;
        return *this;
    }

    inline const ConnectionBuffersReadyDefn::SubscriberPosition subscriberPosition(std::int32_t index) const
    {
        return overlayStruct<ConnectionBuffersReadyDefn::SubscriberPosition>(subscriberPositionOffset(index));
    }

    inline const ConnectionBuffersReadyDefn::SubscriberPosition* subscriberPositions() const
    {
        return &overlayStruct<ConnectionBuffersReadyDefn::SubscriberPosition>(subscriberPositionOffset(0));
    }

    inline std::int32_t length()
    {
        return subscriberPositionOffset(subscriberPositionCount());
    }

private:

    inline util::index_t sourceIdentityOffset() const
    {
        return offsetof(ConnectionBuffersReadyDefn, logFile.logFileData) + m_struct.logFile.logFileLength;
    }

    inline util::index_t subscriberPositionOffset(int index) const
    {
        const util::index_t offset = sourceIdentityOffset();
        const util::index_t startOfPositions = offset + stringGetLength(offset) + (util::index_t)sizeof(std::int32_t);

        return startOfPositions + (index * (util::index_t)sizeof(ConnectionBuffersReadyDefn::SubscriberPosition));
    }
};

}};

#endif