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
#include <atomic>
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
static const std::int64_t PUBLICATION_CLOSED = -4;

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
        return FrameDescriptor::computeMaxMessageLength(termBufferLength());
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @return the length in bytes for each term partition in the log buffer.
     */
    inline std::int32_t termBufferLength() const
    {
        return m_appenders[0]->termBuffer().capacity();
    }

    /**
     * Has this Publication been connected to a Subscription?
     *
     * @return true if this Publication been connected to a Subscription otherwise false.
     */
    inline bool hasBeenConnected()
    {
        return (m_publicationLimit.getVolatile() > 0);
    }

    /**
     * Has this Publication seen an active subscriber recently?
     *
     * @return true if this Publication has seen an active subscriber recently.
     */
    bool isStillConnected();

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    inline bool isClosed() const
    {
        return std::atomic_load_explicit(&m_isClosed, std::memory_order_relaxed);
    }

    /**
     * Get the current position to which the publication has advanced for this stream.
     *
     * @return the current position to which the publication has advanced for this stream or {@link CLOSED}.
     */
    inline std::int64_t position()
    {
        std::int64_t result = PUBLICATION_CLOSED;

        if (!isClosed())
        {
            const std::int64_t rawTail =
                m_appenders[LogBufferDescriptor::activePartitionIndex(m_logMetaDataBuffer)]->rawTailVolatile();

            const std::int32_t termOffset = LogBufferDescriptor::termOffset(rawTail, termBufferLength());

            result = LogBufferDescriptor::computePosition(
                LogBufferDescriptor::termId(rawTail), termOffset, m_positionBitsToShift, m_initialTermId);
        }

        return result;
    }

    /**
     * Get the position limit beyond which this {@link Publication} will be back pressured.
     *
     * This should only be used as a guide to determine when back pressure is likely to be applied.
     *
     * @return the position limit beyond which this {@link Publication} will be back pressured.
     */
    inline std::int64_t positionLimit()
    {
        std::int64_t result = PUBLICATION_CLOSED;

        if (!isClosed())
        {
            result = m_publicationLimit.getVolatile();
        }

        return result;
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     * or {@link CLOSED}.
     */
    inline std::int64_t offer(concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        std::int64_t newPosition = PUBLICATION_CLOSED;

        if (!isClosed())
        {
            const std::int64_t limit = m_publicationLimit.getVolatile();
            const std::int32_t partitionIndex = LogBufferDescriptor::activePartitionIndex(m_logMetaDataBuffer);
            TermAppender *termAppender = m_appenders[partitionIndex].get();
            const std::int64_t rawTail = termAppender->rawTailVolatile();
            const std::int64_t termOffset = rawTail & 0xFFFFFFFF;
            const std::int64_t position =
                LogBufferDescriptor::computeTermBeginPosition(
                    LogBufferDescriptor::termId(rawTail), m_positionBitsToShift, m_initialTermId) + termOffset;

            if (position < limit)
            {
                TermAppender::Result appendResult;
                if (length <= m_maxPayloadLength)
                {
                    termAppender->appendUnfragmentedMessage(appendResult, m_headerWriter, buffer, offset, length);
                }
                else
                {
                    checkForMaxMessageLength(length);
                    termAppender->appendFragmentedMessage(appendResult, m_headerWriter, buffer, offset, length, m_maxPayloadLength);
                }

                newPosition = Publication::newPosition(partitionIndex, static_cast<std::int32_t>(termOffset), position, appendResult);
            }
            else if (0 == limit)
            {
                newPosition = NOT_CONNECTED;
            }
            else
            {
                newPosition = BACK_PRESSURED;
            }
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
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @see BufferClaim::commit
     */
    inline std::int64_t tryClaim(util::index_t length, concurrent::logbuffer::BufferClaim& bufferClaim)
    {
        std::int64_t newPosition = PUBLICATION_CLOSED;

        if (!isClosed())
        {
            checkForMaxMessageLength(length);

            const std::int64_t limit = m_publicationLimit.getVolatile();
            const std::int32_t partitionIndex = LogBufferDescriptor::activePartitionIndex(m_logMetaDataBuffer);
            TermAppender *termAppender = m_appenders[partitionIndex].get();
            const std::int64_t rawTail = termAppender->rawTailVolatile();
            const std::int64_t termOffset = rawTail & 0xFFFFFFFF;
            const std::int64_t position =
                LogBufferDescriptor::computeTermBeginPosition(
                    LogBufferDescriptor::termId(rawTail), m_positionBitsToShift, m_initialTermId) + termOffset;

            if (position < limit)
            {
                TermAppender::Result claimResult;
                termAppender->claim(claimResult, m_headerWriter, length, bufferClaim);
                newPosition = Publication::newPosition(partitionIndex, static_cast<std::int32_t>(termOffset), position, claimResult);
            }
            else if (0 == limit)
            {
                newPosition = NOT_CONNECTED;
            }
            else
            {
                newPosition = BACK_PRESSURED;
            }
        }

        return newPosition;
    }

    /// @cond HIDDEN_SYMBOLS
    inline void close()
    {
        std::atomic_store_explicit(&m_isClosed, true, std::memory_order_relaxed);
    }
    /// @endcond

private:
    ClientConductor& m_conductor;
    AtomicBuffer& m_logMetaDataBuffer;
    const std::string m_channel;
    std::int64_t m_registrationId;
    std::int32_t m_streamId;
    std::int32_t m_sessionId;
    std::int32_t m_initialTermId;
    std::int32_t m_maxPayloadLength;
    std::int32_t m_positionBitsToShift;
    ReadablePosition<UnsafeBufferPosition> m_publicationLimit;
    std::atomic<bool> m_isClosed = { false };

    std::unique_ptr<TermAppender> m_appenders[3];
    HeaderWriter m_headerWriter;

    std::int64_t newPosition(
        int index,
        std::int32_t currentTail,
        std::int64_t position,
        const TermAppender::Result& result)
    {
        std::int64_t newPosition = ADMIN_ACTION;

        if (result.termOffset > 0)
        {
            newPosition = (position - currentTail) + result.termOffset;

        }
        else if (result.termOffset == TERM_APPENDER_TRIPPED)
        {
            const int nextIndex = LogBufferDescriptor::nextPartitionIndex(index);
            const int nextNextIndex = LogBufferDescriptor::nextPartitionIndex(nextIndex);

            m_appenders[nextIndex]->tailTermId(result.termId + 1);
            m_appenders[nextNextIndex]->statusOrdered(LogBufferDescriptor::NEEDS_CLEANING);
            LogBufferDescriptor::activePartitionIndex(m_logMetaDataBuffer, nextIndex);
        }

        return newPosition;
    }

    void checkForMaxMessageLength(const util::index_t length)
    {
        if (length > m_maxPayloadLength)
        {
            throw util::IllegalStateException(
                util::strPrintf("Encoded message exceeds maxMessageLength of %d, length=%d",
                    m_maxPayloadLength, length), SOURCEINFO);
        }
    }
};

}

#endif