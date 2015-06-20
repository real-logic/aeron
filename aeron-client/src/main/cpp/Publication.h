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

#ifndef INCLUDED_AERON_PUBLICATION__
#define INCLUDED_AERON_PUBLICATION__

#include <iostream>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <concurrent/logbuffer/TermAppender.h>
#include <concurrent/status/UnsafeBufferPosition.h>
#include "LogBuffers.h"

namespace aeron {

using namespace aeron::concurrent::status;

class ClientConductor;

static const std::int64_t PUBLICATION_NOT_CONNECTED = -1;
static const std::int64_t PUBLICATION_BACK_PRESSURE = -2;

class Publication
{
public:

    Publication(
        ClientConductor& conductor,
        const std::string& channel,
        std::int64_t registrationId,
        std::int32_t streamId,
        std::int32_t sessionId,
        UnsafeBufferPosition& publicationLimit,
        LogBuffers& buffers);

    virtual ~Publication();

    inline const std::string& channel() const
    {
        return m_channel;
    }

    inline std::int32_t streamId() const
    {
        return m_streamId;
    }

    inline std::int32_t sessionId() const
    {
        return m_sessionId;
    }

    inline std::int64_t registrationId() const
    {
        return m_registrationId;
    }

    inline util::index_t maxMessageLength() const
    {
        return m_appenders[0]->maxMessageLength();
    }

    inline std::int64_t position()
    {
        const std::int32_t initialTermId = LogBufferDescriptor::initialTermId(m_logMetaDataBuffer);
        const std::int32_t activeTermId = LogBufferDescriptor::activeTermId(m_logMetaDataBuffer);
        const std::int32_t currentTail =
            m_appenders[LogBufferDescriptor::indexByTerm(initialTermId, activeTermId)]->rawTailVolatile();

        return LogBufferDescriptor::computePosition(activeTermId, currentTail, m_positionBitsToShift, initialTermId);
    }

    inline std::int64_t offer(concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        const std::int32_t initialTermId = LogBufferDescriptor::initialTermId(m_logMetaDataBuffer);
        const std::int32_t activeTermId = LogBufferDescriptor::activeTermId(m_logMetaDataBuffer);
        const int activeIndex = LogBufferDescriptor::indexByTerm(initialTermId, activeTermId);
        TermAppender* appender = m_appenders[activeIndex].get();
        const std::int32_t currentTail = appender->rawTailVolatile();
        const std::int64_t position = LogBufferDescriptor::computePosition(activeTermId, currentTail, m_positionBitsToShift, initialTermId);
        const std::int32_t capacity = appender->termBuffer().capacity();

        const std::int64_t limit = m_publicationLimit.getVolatile();
        std::int64_t newPosition = limit > 0 ? PUBLICATION_BACK_PRESSURE : PUBLICATION_NOT_CONNECTED;

        if (currentTail < capacity && position < limit)
        {
            const std::int32_t nextOffset = appender->append(buffer, offset, length);
            newPosition = Publication::newPosition(activeTermId, activeIndex, currentTail, position, nextOffset);
        }

        return newPosition;
    }

    inline std::int64_t offer(concurrent::AtomicBuffer& buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    inline std::int64_t tryClaim(util::index_t length, concurrent::logbuffer::BufferClaim& bufferClaim)
    {
        const std::int32_t initialTermId = LogBufferDescriptor::initialTermId(m_logMetaDataBuffer);
        const std::int32_t activeTermId = LogBufferDescriptor::activeTermId(m_logMetaDataBuffer);
        const int activeIndex = LogBufferDescriptor::indexByTerm(initialTermId, activeTermId);
        TermAppender* appender = m_appenders[activeIndex].get();
        const std::int32_t currentTail = appender->rawTailVolatile();
        const std::int64_t position = LogBufferDescriptor::computePosition(activeTermId, currentTail, m_positionBitsToShift, initialTermId);
        const std::int32_t capacity = appender->termBuffer().capacity();

        const std::int64_t limit = m_publicationLimit.getVolatile();
        std::int64_t newPosition = limit > 0 ? PUBLICATION_BACK_PRESSURE : PUBLICATION_NOT_CONNECTED;

        if (currentTail < capacity && position < limit)
        {
            const std::int32_t nextOffset = appender->claim(length, bufferClaim);
            newPosition = Publication::newPosition(activeTermId, activeIndex, currentTail, position, nextOffset);
        }

        return newPosition;
    }

private:
    ClientConductor& m_conductor;
    const std::string m_channel;
    std::int64_t m_registrationId;
    std::int32_t m_streamId;
    std::int32_t m_sessionId;
    ReadablePosition<UnsafeBufferPosition> m_publicationLimit;

    AtomicBuffer& m_logMetaDataBuffer;
    std::unique_ptr<TermAppender> m_appenders[3];
    std::int32_t m_positionBitsToShift;

    std::int64_t newPosition(
        std::int32_t activeTermId, int activeIndex, std::int32_t currentTail, std::int64_t position, std::int32_t nextOffset)
    {
        std::int64_t newPosition;

        switch (nextOffset)
        {
            case TERM_APPENDER_TRIPPED:
            {
                const std::int32_t newTermId = activeTermId + 1;
                const int nextIndex = LogBufferDescriptor::nextPartitionIndex(activeIndex);
                const int nextNextIndex = LogBufferDescriptor::nextPartitionIndex(nextIndex);

                LogBufferDescriptor::defaultHeaderTermId(m_logMetaDataBuffer, nextIndex, newTermId);

                LogBufferDescriptor::defaultHeaderTermId(m_logMetaDataBuffer, nextNextIndex, newTermId + 1);
                m_appenders[nextNextIndex]->statusOrdered(LogBufferDescriptor::NEEDS_CLEANING);
                LogBufferDescriptor::activeTermId(m_logMetaDataBuffer, newTermId);
            }
            // fall through

            case TERM_APPENDER_FAILED:
                newPosition = PUBLICATION_BACK_PRESSURE;
                break;

            default:
                newPosition = (position - currentTail) + nextOffset;
        }

        return newPosition;
    }
};

}

#endif