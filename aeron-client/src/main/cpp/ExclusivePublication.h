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

#ifndef AERON_EXCLUSIVEPUBLICATION_H
#define AERON_EXCLUSIVEPUBLICATION_H

#include <iostream>
#include <atomic>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/ExclusiveBufferClaim.h>
#include <concurrent/logbuffer/ExclusiveTermAppender.h>
#include <concurrent/status/UnsafeBufferPosition.h>
#include "Publication.h"
#include "LogBuffers.h"

namespace aeron {

using namespace aeron::concurrent::status;

/**
 * Aeron Publisher API for sending messages to subscribers of a given channel and streamId pair. ExclusivePublications
 * each get their own session id so multiple can be concurrently active on the same media driver as independent streams.
 *
 * {@link ExclusivePublication}s are created via the {@link Aeron#addExclusivePublication(String, int)} method,
 * and messages are sent via one of the {@link #offer(DirectBuffer)} methods, or a
 * {@link #tryClaim(int, ExclusiveBufferClaim)} and {@link ExclusiveBufferClaim#commit()} method combination.
 *
 * {@link ExclusivePublication}s have the potential to provide greater throughput than {@link Publication}s.
 *
 * The APIs used try claim and offer are non-blocking.
 *
 * <b>Note:</b> ExclusivePublication instances are NOT threadsafe for offer and try claim method but are for position.
 *
 * @see Aeron#addExclusivePublication(String, int)
 * @see ExclusiveBufferClaim
 */
class ExclusivePublication
{
public:

    /// @cond HIDDEN_SYMBOLS
    ExclusivePublication(
        ClientConductor& conductor,
        const std::string& channel,
        std::int64_t registrationId,
        std::int64_t correlationId,
        std::int32_t streamId,
        std::int32_t sessionId,
        UnsafeBufferPosition& publicationLimit,
        std::shared_ptr<LogBuffers> buffers);
    /// @endcond

    virtual ~ExclusivePublication();

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
     * The initial term id assigned when this Publication was created. This can be used to determine how many
     * terms have passed since creation.
     *
     * @return the initial term id.
     */
    inline std::int32_t initialTermId() const
    {
        return m_initialTermId;
    }

    /**
     * Get the original registration used to register this Publication with the media driver by the first publisher.
     *
     * @return the original registrationId of the publication.
     */
    inline std::int64_t originalRegistrationId() const
    {
        return m_originalRegistrationId;
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
     * Is this Publication the original instance added to the driver? If not then it was added after another client
     * has already added the publication.
     *
     * @return true if this instance is the first added otherwise false.
     */
    inline bool isOriginal() const
    {
        return m_originalRegistrationId == m_registrationId;
    }

    /**
     * Maximum message length supported in bytes.
     *
     * @return maximum message length supported in bytes.
     */
    inline util::index_t maxMessageLength() const
    {
        return m_maxMessageLength;
    }

    /**
     * Maximum length of a message payload that fits within a message fragment.
     *
     * This is he MTU length minus the message fragment header length.
     *
     * @return maximum message fragment payload length.
     */
    inline util::index_t maxPayloadLength() const
    {
        return m_maxPayloadLength;
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
     * Has this Publication seen an active subscriber recently?
     *
     * @return true if this Publication has seen an active subscriber recently.
     */
    inline bool isConnected() const
    {
        return !isClosed() && isPublicationConnected(LogBufferDescriptor::timeOfLastStatusMessage(m_logMetaDataBuffer));
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    inline bool isClosed() const
    {
        return std::atomic_load_explicit(&m_isClosed, std::memory_order_acquire);
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
            const std::int64_t rawTail = LogBufferDescriptor::rawTailVolatile(m_logMetaDataBuffer);
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
     * @param reservedValueSupplier for the frame.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    inline std::int64_t offer(
        concurrent::AtomicBuffer& buffer,
        util::index_t offset,
        util::index_t length,
        const on_reserved_value_supplier_t& reservedValueSupplier)
    {
        std::int64_t newPosition = PUBLICATION_CLOSED;

        if (!isClosed())
        {
            const std::int64_t limit = m_publicationLimit.getVolatile();
            ExclusiveTermAppender *termAppender = m_appenders[m_activePartitionIndex].get();
            const std::int64_t position = m_termBeginPosition + m_termOffset;

            if (position < limit)
            {
                std::int32_t result;
                if (length <= m_maxPayloadLength)
                {
                    result = termAppender->appendUnfragmentedMessage(
                        m_termId, m_termOffset, m_headerWriter, buffer, offset, length, reservedValueSupplier);
                }
                else
                {
                    checkForMaxMessageLength(length);
                    result = termAppender->appendFragmentedMessage(
                        m_termId, m_termOffset, m_headerWriter, buffer, offset, length, m_maxPayloadLength, reservedValueSupplier);
                }

                newPosition = ExclusivePublication::newPosition(result);
            }
            else if (isPublicationConnected(LogBufferDescriptor::timeOfLastStatusMessage(m_logMetaDataBuffer)))
            {
                newPosition = BACK_PRESSURED;
            }
            else
            {
                newPosition = NOT_CONNECTED;
            }
        }

        return newPosition;
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    inline std::int64_t offer(concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        return offer(buffer, offset, length, DEFAULT_RESERVED_VALUE_SUPPLIER);
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
    inline std::int64_t tryClaim(util::index_t length, concurrent::logbuffer::ExclusiveBufferClaim& bufferClaim)
    {
        std::int64_t newPosition = PUBLICATION_CLOSED;

        if (AERON_COND_EXPECT((!isClosed()), true))
        {
            checkForMaxPayloadLength(length);

            const std::int64_t limit = m_publicationLimit.getVolatile();
            ExclusiveTermAppender *termAppender = m_appenders[m_activePartitionIndex].get();
            const std::int64_t position = m_termBeginPosition + m_termOffset;

            if (AERON_COND_EXPECT((position < limit), true))
            {
                const std::int32_t result = termAppender->claim(m_termId, m_termOffset, m_headerWriter, length, bufferClaim);
                newPosition = ExclusivePublication::newPosition(result);
            }
            else if (isPublicationConnected(LogBufferDescriptor::timeOfLastStatusMessage(m_logMetaDataBuffer)))
            {
                newPosition = BACK_PRESSURED;
            }
            else
            {
                newPosition = NOT_CONNECTED;
            }
        }

        return newPosition;
    }

    /**
     * Add a destination manually to a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to add
     */
    void addDestination(const std::string& endpointChannel);

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to remove
     */
    void removeDestination(const std::string& endpointChannel);

    /// @cond HIDDEN_SYMBOLS
    inline void close()
    {
        std::atomic_store_explicit(&m_isClosed, true, std::memory_order_release);
    }
    /// @endcond

private:
    ClientConductor& m_conductor;
    AtomicBuffer& m_logMetaDataBuffer;

    const std::string m_channel;
    std::int64_t m_registrationId;
    std::int64_t m_originalRegistrationId;
    std::int32_t m_streamId;
    std::int32_t m_sessionId;
    std::int32_t m_initialTermId;
    std::int32_t m_maxPayloadLength;
    std::int32_t m_maxMessageLength;
    std::int32_t m_positionBitsToShift;

    std::int64_t m_termBeginPosition;
    std::int32_t m_activePartitionIndex;
    std::int32_t m_termId;
    std::int32_t m_termOffset;

    ReadablePosition<UnsafeBufferPosition> m_publicationLimit;
    std::atomic<bool> m_isClosed = { false };

    std::shared_ptr<LogBuffers> m_logbuffers;
    std::unique_ptr<ExclusiveTermAppender> m_appenders[3];
    HeaderWriter m_headerWriter;

    inline std::int64_t newPosition(const std::int32_t resultingOffset)
    {
        if (resultingOffset > 0)
        {
            m_termOffset = resultingOffset;

            return m_termBeginPosition + resultingOffset;
        }
        else
        {
            const int nextIndex = LogBufferDescriptor::nextPartitionIndex(m_activePartitionIndex);
            const std::int32_t nextTermId = m_termId + 1;

            m_activePartitionIndex = nextIndex;
            m_termOffset = 0;
            m_termId = nextTermId;
            m_termBeginPosition = LogBufferDescriptor::computeTermBeginPosition(nextTermId, m_positionBitsToShift, m_initialTermId);

            m_appenders[nextIndex]->tailTermId(nextTermId);
            LogBufferDescriptor::activePartitionIndex(m_logMetaDataBuffer, nextIndex);

            return ADMIN_ACTION;
        }
    }

    inline void checkForMaxMessageLength(const util::index_t length) const
    {
        if (length > m_maxMessageLength)
        {
            throw util::IllegalStateException(
                util::strPrintf("Encoded message exceeds maxMessageLength of %d, length=%d",
                    m_maxMessageLength, length), SOURCEINFO);
        }
    }

    inline void checkForMaxPayloadLength(const util::index_t length) const
    {
        if (AERON_COND_EXPECT((length > m_maxPayloadLength), false))
        {
            throw util::IllegalStateException(
                util::strPrintf("Encoded message exceeds maxPayloadLength of %d, length=%d",
                    m_maxPayloadLength, length), SOURCEINFO);
        }
    }

    bool isPublicationConnected(std::int64_t timeOfLastStatusMessage) const;
};

}

#endif
