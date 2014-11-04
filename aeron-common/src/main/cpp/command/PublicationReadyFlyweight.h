/*
 * Copyright 2014 Real Logic Ltd.
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
#include <common/Flyweight.h>
#include "ReadyFlyweight.h"
#include <common/TermHelper.h>

namespace aeron { namespace common { namespace command {

/**
* Message to denote that new buffers have been added for a subscription.
*
* @see uk.co.real_logic.aeron.common.command.ControlProtocolEvents
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
* |                           Term ID                             |
* +---------------------------------------------------------------+
* |                   Position Indicator Offset                   |
* +---------------------------------------------------------------+
* |                           MTU Length                          |
* +---------------------------------------------------------------+
* |                          File Offset 0                        |
* +---------------------------------------------------------------+
* |                          File Offset 1                        |
* +---------------------------------------------------------------+
* |                          File Offset 2                        |
* +---------------------------------------------------------------+
* |                          File Offset 3                        |
* +---------------------------------------------------------------+
* |                          File Offset 4                        |
* +---------------------------------------------------------------+
* |                          File Offset 5                        |
* +---------------------------------------------------------------+
* |                             Length 0                          |
* +---------------------------------------------------------------+
* |                             Length 1                          |
* +---------------------------------------------------------------+
* |                             Length 2                          |
* +---------------------------------------------------------------+
* |                             Length 3                          |
* +---------------------------------------------------------------+
* |                             Length 4                          |
* +---------------------------------------------------------------+
* |                             Length 5                          |
* +---------------------------------------------------------------+
* |                          Location 1 Start                     |
* +---------------------------------------------------------------+
* |                          Location 2 Start                     |
* +---------------------------------------------------------------+
* |                          Location 3 Start                     |
* +---------------------------------------------------------------+
* |                          Location 4 Start                     |
* +---------------------------------------------------------------+
* |                          Location 5 Start                     |
* +---------------------------------------------------------------+
* |                           Channel Start                       |
* +---------------------------------------------------------------+
* |                           Channel End                         |
* +---------------------------------------------------------------+
* |                            Location 0                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 1                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 2                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 3                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 4                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 5                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Channel                          ...
* |                                                             ...
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct PublicationReadyDefn
{
    static const std::int32_t NUM_FILES = 6;
    static const std::int32_t PAYLOAD_BUFFER_COUNT = common::TermHelper::BUFFER_COUNT * 2;
    static const std::int32_t CHANNEL_INDEX = PAYLOAD_BUFFER_COUNT;

    std::int64_t correlationId;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t termId;
    std::int32_t positionIndicatorsCount;
	std::int32_t mtuLength;
    std::int32_t fileOffset[NUM_FILES];
    std::int32_t length[NUM_FILES];
	std::int32_t locationStart[NUM_FILES + 2]; // extra space to store location of channel
};
#pragma pack(pop)

class PublicationReadyFlyweight : public common::Flyweight<PublicationReadyDefn>,
								  public ReadyFlyweight<PublicationReadyFlyweight>
{
public:
	typedef PublicationReadyFlyweight this_t;

	inline PublicationReadyFlyweight(concurrent::AtomicBuffer& buffer, size_t offset)
		: common::Flyweight<PublicationReadyDefn>(buffer, offset)
    {
    }

    inline std::int32_t bufferOffset(std::int32_t index) const
    {
        return m_struct.fileOffset[index];
    }

    inline this_t& bufferOffset(std::int32_t index, std::int32_t value)
    {
        m_struct.fileOffset[index] = value;
        return *this;
    }

    inline std::int32_t bufferLength(std::int32_t index) const
    {
        return m_struct.length[index];
    }

    inline this_t& bufferLength(std::int32_t index, std::int32_t value)
    {
        m_struct.length[index] = value;
        return *this;
    }

    inline std::string location(std::int32_t index) const
    {
        std::int32_t offset;
        if (index == 0)
            offset = (std::int32_t)sizeof(PublicationReadyDefn);
        else
            offset = locationOffset(index);

        std::int32_t length = locationOffset(index+1);

        return stringGetWithoutLength(offset, length);
    }

    inline this_t& location(std::int32_t index, const std::string &value)
    {
        std::int32_t offset;
        if (index == 0)
            offset = (std::int32_t)sizeof(PublicationReadyDefn);
        else
            offset = locationOffset(index);

        offset += stringPutWithoutLength(offset, value);
        locationOffset(index + 1, offset);

        return *this;
    }

    inline std::string channel() const
    {
        return location(PublicationReadyDefn::CHANNEL_INDEX);
    }

    inline this_t& channel(const std::string &value)
    {
        location(PublicationReadyDefn::CHANNEL_INDEX, value);
        return *this;
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

    inline std::int32_t termId() const
    {
        return m_struct.termId;
    }

    inline this_t& termId(std::int32_t value)
    {
        m_struct.termId = value;
        return *this;
    }

    inline std::int32_t positionIndicatorsCount() const
    {
        return m_struct.positionIndicatorsCount;
    }

    inline this_t& positionIndicatorsCount(std::int32_t value)
    {
        m_struct.positionIndicatorsCount = value;
        return *this;
    }

	inline std::int32_t mtuLength() const
	{
		return m_struct.mtuLength;
	}

	inline this_t& mtuLength(std::int32_t value)
	{
		m_struct.mtuLength = value;
		return *this;
	}

    std::int32_t length()
    {
        return locationOffset(PublicationReadyDefn::CHANNEL_INDEX + 1);
    }

private:
    inline std::int32_t locationOffset(std::int32_t index) const
    {
        return m_struct.locationStart[index];
    }

    inline void locationOffset(std::int32_t index, std::int32_t value)
    {
        m_struct.locationStart[index] = value;
    }
};

}}};

#endif