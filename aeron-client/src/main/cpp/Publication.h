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

static const std::int64_t NOT_CONNECTED = -1;
static const std::int64_t BACK_PRESSURED = -2;
static const std::int64_t ADMIN_ACTION = -3;

/**
 * @example BasicPublisher.cpp
 */
/**
 * Aeron Publisher API for sending messages to subscribers of a given channel and streamId pair. Publishers
 * are created via an {@link Aeron} object, and messages are sent via an offer method or a claim and commit
 * method combination.
 * <p>
 * The APIs used to send are all non-blocking.
 * <p>
 * Note: Publication instances are threadsafe and can be shared between publisher threads.
 * @see Aeron#addPublication
 * @see Aeron#findPublication
 */
class Publication
{
public:

    /// @cond HIDDEN_SYMBOLS
    Publication(
        ClientConductor& conductor,
        const std::string& channel,
        std::int64_t registrationId,
        std::int32_t streamId,
        std::int32_t sessionId,
        UnsafeBufferPosition& publicationLimit,
        LogBuffers& buffers);
    /// @endcond

    virtual ~Publication();

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    inline const std::string& channel() const
    {
        return m_channel;
    }

    // TODO:
    // termBufferLength
    // hasBeenConnected
    // - ensureOpen logic
    // isClosed()

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    inline std::int32_t streamId() const
    {
        return m_streamId;
    }

    /**
     * Session under which messages are published. Identifies this Publication instance.
     *
     * @return the session id for this publication.
     */
    inline std::int32_t sessionId() const
    {
        return m_sessionId;
    }

    /**
     * Registration Id returned by Aeron::addPublication when this Publication was added.
     *
     * @return the registrationId of the publication.
     */
    inline std::int64_t registrationId() const
    {
        return m_registrationId;
    }

    /**
     * Maximum message length supported in bytes.
     *
     * @return maximum message length supported in bytes.
     */
    inline util::index_t maxMessageLength() const
    {
        return m_appenders[0]->maxMessageLength();
    }

    /**
     * Get the current position to which the publication has advanced for this stream.
     *
     * @return the current position to which the publication has advanced for this stream.
     */
    inline std::int64_t position()
    {
        const std::int32_t initialTermId = LogBufferDescriptor::initialTermId(m_logMetaDataBuffer);
        const std::int32_t activeTermId = LogBufferDescriptor::activeTermId(m_logMetaDataBuffer);
        const std::int32_t currentTail =
            m_appenders[LogBufferDescriptor::indexByTerm(initialTermId, activeTermId)]->rawTailVolatile();

        return LogBufferDescriptor::computePosition(activeTermId, currentTail, m_positionBitsToShift, initialTermId);
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED} or {@link #ADMIN_ACTION}.
     */
    inline std::int64_t offer(concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        const std::int32_t initialTermId = LogBufferDescriptor::initialTermId(m_logMetaDataBuffer);
        const std::int32_t activeTermId = LogBufferDescriptor::activeTermId(m_logMetaDataBuffer);
        const int activeIndex = LogBufferDescriptor::indexByTerm(initialTermId, activeTermId);
        TermAppender* appender = m_appenders[activeIndex].get();
        const std::int32_t currentTail = appender->rawTailVolatile();
        const std::int64_t position = LogBufferDescriptor::computePosition(activeTermId, currentTail, m_positionBitsToShift, initialTermId);

        const std::int64_t limit = m_publicationLimit.getVolatile();
        std::int64_t newPosition = BACK_PRESSURED;

        if (position < limit)
        {
            const std::int32_t nextOffset = appender->append(buffer, offset, length);
            newPosition = Publication::newPosition(activeTermId, activeIndex, currentTail, position, nextOffset);
        }
        else if (0 == limit)
        {
            newPosition = NOT_CONNECTED;
        }

        return newPosition;
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return The new stream position on success, otherwise {@link BACK_PRESSURED} or {@link NOT_CONNECTED}.
     */
    inline std::int64_t offer(concurrent::AtomicBuffer& buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     *
     * @code
     *     BufferClaim bufferClaim; // Can be stored and reused to avoid allocation
     *
     *     if (publication->tryClaim(messageLength, bufferClaim) > 0)
     *     {
     *         try
     *         {
     *              AtomicBuffer& buffer = bufferClaim.buffer();
     *              const index_t offset = bufferClaim.offset();
     *
     *              // Work with buffer directly or wrap with a flyweight
     *         }
     *         finally
     *         {
     *             bufferClaim.commit();
     *         }
     *     }
     * @endcode
     *
     * @param length      of the range to claim, in bytes..
     * @param bufferClaim to be populate if the claim succeeds.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED} or {@link #ADMIN_ACTION}.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @see BufferClaim::commit
     */
    inline std::int64_t tryClaim(util::index_t length, concurrent::logbuffer::BufferClaim& bufferClaim)
    {
        const std::int32_t initialTermId = LogBufferDescriptor::initialTermId(m_logMetaDataBuffer);
        const std::int32_t activeTermId = LogBufferDescriptor::activeTermId(m_logMetaDataBuffer);
        const int activeIndex = LogBufferDescriptor::indexByTerm(initialTermId, activeTermId);
        TermAppender* appender = m_appenders[activeIndex].get();
        const std::int32_t currentTail = appender->rawTailVolatile();
        const std::int64_t position = LogBufferDescriptor::computePosition(activeTermId, currentTail, m_positionBitsToShift, initialTermId);

        const std::int64_t limit = m_publicationLimit.getVolatile();
        std::int64_t newPosition = BACK_PRESSURED;

        if (position < limit)
        {
            const std::int32_t nextOffset = appender->claim(length, bufferClaim);
            newPosition = Publication::newPosition(activeTermId, activeIndex, currentTail, position, nextOffset);
        }
        else if (0 == limit)
        {
            newPosition = NOT_CONNECTED;
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

                newPosition = ADMIN_ACTION;
                break;
            }

            case TERM_APPENDER_FAILED:
                newPosition = BACK_PRESSURED;
                break;

            default:
                newPosition = (position - currentTail) + nextOffset;
        }

        return newPosition;
    }
};

}

#endif