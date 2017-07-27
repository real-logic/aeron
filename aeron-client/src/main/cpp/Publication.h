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
        std::int64_t originalRegistrationId,
        std::int32_t streamId,
        std::int32_t sessionId,
        UnsafeBufferPosition& publicationLimit,
        std::shared_ptr<LogBuffers> buffers);
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
                    termAppender->appendUnfragmentedMessage(
                        appendResult, m_headerWriter, buffer, offset, length, reservedValueSupplier);
                }
                else
                {
                    checkForMaxMessageLength(length);
                    termAppender->appendFragmentedMessage(
                        appendResult, m_headerWriter, buffer, offset, length, m_maxPayloadLength, reservedValueSupplier);
                }

                newPosition = Publication::newPosition(partitionIndex, static_cast<std::int32_t>(termOffset), position, appendResult);
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
    inline std::int64_t tryClaim(util::index_t length, concurrent::logbuffer::BufferClaim& bufferClaim)
    {
        std::int64_t newPosition = PUBLICATION_CLOSED;

        if (!isClosed())
        {
            checkForMaxPayloadLength(length);

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
    ReadablePosition<UnsafeBufferPosition> m_publicationLimit;
    std::atomic<bool> m_isClosed = { false };

    std::shared_ptr<LogBuffers> m_logbuffers;
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

            m_appenders[nextIndex]->tailTermId(result.termId + 1);
            LogBufferDescriptor::activePartitionIndex(m_logMetaDataBuffer, nextIndex);
        }

        return newPosition;
    }

    void checkForMaxMessageLength(const util::index_t length)
    {
        if (length > m_maxMessageLength)
        {
            throw util::IllegalStateException(
                util::strPrintf("Encoded message exceeds maxMessageLength of %d, length=%d",
                    m_maxMessageLength, length), SOURCEINFO);
        }
    }

    void checkForMaxPayloadLength(const util::index_t length)
    {
        if (length > m_maxPayloadLength)
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
