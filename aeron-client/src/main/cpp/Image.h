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

#ifndef AERON_IMAGE_H
#define AERON_IMAGE_H

#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/LogBufferDescriptor.h>
#include <concurrent/logbuffer/Header.h>
#include <concurrent/logbuffer/TermReader.h>
#include <concurrent/logbuffer/TermBlockScanner.h>
#include <concurrent/status/UnsafeBufferPosition.h>
#include <algorithm>
#include "LogBuffers.h"

namespace aeron {

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::status;

static UnsafeBufferPosition NULL_POSITION;

/**
 * Represents a replicated publication {@link Image} from a publisher to a {@link Subscription}.
 * Each {@link Image} identifies a source publisher by session id.
 *
 * Is an overlay on the LogBuffers and Position. So, can be effectively copied and moved.
 */
class Image
{
public:
    typedef Image this_t;

    Image() :
        m_header(0, 0),
        m_subscriberPosition(NULL_POSITION)
    {
    }

    /**
     * Construct a new image over a log to represent a stream of messages from a {@link Publication}.
     *
     * @param sessionId          of the stream of messages.
     * @param initialPosition    at which the subscriber is joining the stream.
     * @param subscriberPosition for indicating the position of the subscriber in the stream.
     * @param logBuffers         containing the stream of messages.
     * @param correlationId      of the request to the media driver.
     */
    Image(
        std::int32_t sessionId,
        std::int64_t initialPosition,
        std::int64_t correlationId,
        UnsafeBufferPosition& subscriberPosition,
        std::shared_ptr<LogBuffers> logBuffers) :
        m_header(
            LogBufferDescriptor::initialTermId(logBuffers->atomicBuffer(LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX)),
            logBuffers->atomicBuffer(0).capacity()),
        m_subscriberPosition(subscriberPosition),
        m_logBuffers(logBuffers),
        m_correlationId(correlationId),
        m_sessionId(sessionId)
    {
        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            m_termBuffers[i] = logBuffers->atomicBuffer(i);
        }

        const util::index_t capacity = m_termBuffers[0].capacity();

        m_termLengthMask = capacity - 1;
        m_positionBitsToShift = BitUtil::numberOfTrailingZeroes(capacity);
        m_subscriberPosition.setOrdered(initialPosition);
    }

    Image(Image& image) :
        m_header(image.m_header),
        m_subscriberPosition(image.m_subscriberPosition)
    {
        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            m_termBuffers[i].wrap(image.m_termBuffers[i]);
        }

        m_subscriberPosition.wrap(image.m_subscriberPosition);
        m_logBuffers = image.m_logBuffers;
        m_correlationId = image.m_correlationId;
        m_sessionId = image.m_sessionId;
        m_termLengthMask = image.m_termLengthMask;
        m_positionBitsToShift = image.m_positionBitsToShift;
    }

    Image& operator=(Image& image)
    {
        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            m_termBuffers[i].wrap(image.m_termBuffers[i]);
        }

        m_header = image.m_header;
        m_subscriberPosition.wrap(image.m_subscriberPosition);
        m_logBuffers = image.m_logBuffers;
        m_correlationId = image.m_correlationId;
        m_sessionId = image.m_sessionId;
        m_termLengthMask = image.m_termLengthMask;
        m_positionBitsToShift = image.m_positionBitsToShift;
        return *this;
    }

    Image& operator=(Image&& image)
    {
        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            m_termBuffers[i].wrap(image.m_termBuffers[i]);
        }

        m_header = image.m_header;
        m_subscriberPosition.wrap(image.m_subscriberPosition);
        m_logBuffers = std::move(image.m_logBuffers);
        m_correlationId = image.m_correlationId;
        m_sessionId = image.m_sessionId;
        m_termLengthMask = image.m_termLengthMask;
        m_positionBitsToShift = image.m_positionBitsToShift;
        return *this;
    }

    virtual ~Image()
    {
    }

    /**
     * The sessionId for the steam of messages.
     *
     * @return the sessionId for the steam of messages.
     */
    inline std::int32_t sessionId()
    {
        return m_sessionId;
    }

    /**
     * The correlationId for identification of the image with the media driver.
     *
     * @return the correlationId for identification of the image with the media driver.
     */
    inline std::int64_t correlationId()
    {
        return m_correlationId;
    }

    /**
     * The initial term at which the stream started for this session.
     *
     * @return the initial term id.
     */
    inline std::int32_t initialTermId()
    {
        return m_header.initialTermId();
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the fragment_handler_t up to a limited number of fragments as specified.
     *
     * @param fragmentHandler to which messages are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see fragment_handler_t
     */
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

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the block_handler_t up to a limited number of bytes.
     *
     * @param blockHandler     to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     *
     * @see block_handler_t
     */
    int blockPoll(const block_handler_t& blockHandler, int blockLengthLimit)
    {
        const std::int64_t position = m_subscriberPosition.get();
        const std::int32_t termOffset = (std::int32_t)position & m_termLengthMask;
        AtomicBuffer& termBuffer = m_termBuffers[LogBufferDescriptor::indexByPosition(position, m_positionBitsToShift)];
        const std::int32_t limit = std::min(termOffset + blockLengthLimit, termBuffer.capacity());

        const std::int32_t resultingOffset = TermBlockScanner::scan(termBuffer, termOffset, limit);

        const std::int32_t bytesConsumed = resultingOffset - termOffset;

        if (resultingOffset > termOffset)
        {
            const std::int32_t termId = termBuffer.getInt32(termOffset + DataFrameHeader::TERM_ID_FIELD_OFFSET);

            blockHandler(termBuffer, termOffset, bytesConsumed, m_sessionId, termId);
            m_subscriberPosition.setOrdered(position + bytesConsumed);
        }

        return bytesConsumed;
    }

    std::shared_ptr<LogBuffers> logBuffers()
    {
        return m_logBuffers;
    }

private:
    AtomicBuffer m_termBuffers[LogBufferDescriptor::PARTITION_COUNT];
    Header m_header;
    Position<UnsafeBufferPosition> m_subscriberPosition;
    std::shared_ptr<LogBuffers> m_logBuffers;

    std::int64_t m_correlationId;
    std::int32_t m_sessionId;
    std::int32_t m_termLengthMask;
    std::int32_t m_positionBitsToShift;
};

}

#endif //AERON_IMAGE_H
