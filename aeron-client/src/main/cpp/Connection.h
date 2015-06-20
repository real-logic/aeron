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

#ifndef AERON_CONNECTION_H
#define AERON_CONNECTION_H

#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/LogBufferDescriptor.h>
#include <concurrent/logbuffer/Header.h>
#include <concurrent/logbuffer/TermReader.h>
#include <concurrent/status/UnsafeBufferPosition.h>
#include "LogBuffers.h"

namespace aeron {

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::status;

static UnsafeBufferPosition NULL_POSITION;

class Connection
{
public:
    Connection() :
        m_header(0, 0),
        m_subscriberPosition(NULL_POSITION)
    {
    }

    Connection(
        std::int32_t sessionId,
        std::int64_t initialPosition,
        std::int64_t correlationId,
        UnsafeBufferPosition& subscriberPosition,
        LogBuffers& logBuffers) :
        m_header(
            LogBufferDescriptor::initialTermId(logBuffers.atomicBuffer(LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX)),
            logBuffers.atomicBuffer(0).capacity()),
        m_subscriberPosition(subscriberPosition),
        m_correlationId(correlationId),
        m_sessionId(sessionId)
    {
        for (int i = LogBufferDescriptor::PARTITION_COUNT; i >= 0; i--)
        {
            m_termBuffers[i] = logBuffers.atomicBuffer(i);
        }

        const util::index_t capacity = m_termBuffers[0].capacity();

        m_termLengthMask = capacity - 1;
        m_positionBitsToShift = BitUtil::numberOfTrailingZeroes(capacity);
    }

    Connection(Connection&) = delete;
    Connection& operator=(Connection&) = delete;

    Connection& operator=(Connection&& connection)
    {
        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            m_termBuffers[i].wrap(connection.m_termBuffers[i]);
        }

        m_header = connection.m_header;
        m_subscriberPosition.wrap(connection.m_subscriberPosition);
        m_correlationId = connection.m_correlationId;
        m_sessionId = connection.m_sessionId;
        m_termLengthMask = connection.m_termLengthMask;
        m_positionBitsToShift = connection.m_positionBitsToShift;
        return *this;
    }

    virtual ~Connection()
    {
    }

    inline std::int32_t sessionId()
    {
        return m_sessionId;
    }

    inline std::int64_t correlationId()
    {
        return m_correlationId;
    }

    int poll(const fragment_handler_t& fragmentHandler, int fragmentLimit)
    {
        const std::int64_t position = m_subscriberPosition.get();
        const std::int32_t termOffset = (std::int32_t)position & m_termLengthMask;
        AtomicBuffer& termBuffer = m_termBuffers[LogBufferDescriptor::indexByPosition(position, m_positionBitsToShift)];

        const TermReader::ReadOutcome readOutcome =
            TermReader::read(termBuffer, termOffset, fragmentHandler, fragmentLimit, m_header);

        const std::int64_t newPosition = position + (readOutcome.offset - termOffset);
        if (newPosition > position)
        {
            m_subscriberPosition.setOrdered(newPosition);
        }

        return readOutcome.fragmentsRead;
    }

private:
    AtomicBuffer m_termBuffers[LogBufferDescriptor::PARTITION_COUNT];
    Header m_header;
    Position<UnsafeBufferPosition> m_subscriberPosition;

    std::int64_t m_correlationId;
    std::int32_t m_sessionId;
    std::int32_t m_termLengthMask;
    std::int32_t m_positionBitsToShift;
};

}

#endif //AERON_CONNECTION_H
