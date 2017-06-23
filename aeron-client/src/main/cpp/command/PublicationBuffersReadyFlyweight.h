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
#ifndef INCLUDED_AERON_COMMAND_PUBLICATIONREADYFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_PUBLICATIONREADYFLYWEIGHT__

#include <cstdint>
#include <stddef.h>
#include "Flyweight.h"

namespace aeron { namespace command {

/**
* Message to denote that new buffers have been added for a publication.
*
* @see ControlProtocolEvents
*
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
* |                        Registration ID                        |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Session ID                           |
* +---------------------------------------------------------------+
* |                           Stream ID                           |
* +---------------------------------------------------------------+
* |                   Position Limit Counter Id                   |
* +---------------------------------------------------------------+
* |                         Log File Length                       |
* +---------------------------------------------------------------+
* |                          Log File Name                      ...
* ...                                                             |
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct PublicationBuffersReadyDefn
{
    std::int64_t correlationId;
    std::int64_t registrationId;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t positionLimitCounterId;
    std::int32_t logFileLength;
    std::int8_t  logFileData[1];
};
#pragma pack(pop)

class PublicationBuffersReadyFlyweight : public Flyweight<PublicationBuffersReadyDefn>
{
public:
    typedef PublicationBuffersReadyFlyweight this_t;

    inline PublicationBuffersReadyFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset) :
        Flyweight<PublicationBuffersReadyDefn>(buffer, offset)
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

    inline std::int64_t registrationId() const
    {
        return m_struct.registrationId;
    }

    inline this_t& registrationId(std::int64_t value)
    {
        m_struct.registrationId = value;
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

    inline std::int32_t positionLimitCounterId() const
    {
        return m_struct.positionLimitCounterId;
    }

    inline this_t& positionLimitCounterId(std::int32_t value)
    {
        m_struct.positionLimitCounterId = value;
        return *this;
    }

    inline std::string logFileName() const
    {
        return stringGet(offsetof(PublicationBuffersReadyDefn, logFileLength));
    }

    inline this_t& logFileName(const std::string& value)
    {
        stringPut(offsetof(PublicationBuffersReadyDefn, logFileLength), value);
        return *this;
    }

    std::int32_t length() const
    {
        return offsetof(PublicationBuffersReadyDefn, logFileData) + m_struct.logFileLength;
    }
};

}}

#endif
