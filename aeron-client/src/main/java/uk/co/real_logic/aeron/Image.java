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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.logbuffer.*;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.ManagedResource;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;

import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.logbuffer.TermReader.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.TERM_ID_FIELD_OFFSET;

/**
 * Represents a replicated publication {@link Image} from a publisher to a {@link Subscription}.
 * Each {@link Image} identifies a source publisher by session id.
 */
public class Image
{
    private final long correlationId;
    private final int sessionId;
    private final int termLengthMask;
    private final int positionBitsToShift;

    private final Position subscriberPosition;
    private final UnsafeBuffer[] termBuffers;
    private final Header header;
    private final ErrorHandler errorHandler;
    private final LogBuffers logBuffers;

    /**
     * Construct a new image over a log to represent a stream of messages from a {@link Publication}.
     *
     * @param sessionId          of the stream of messages.
     * @param initialPosition    at which the subscriber is joining the stream.
     * @param subscriberPosition for indicating the position of the subscriber in the stream.
     * @param logBuffers         containing the stream of messages.
     * @param errorHandler       to be called if an error occurs when polling for messages.
     * @param correlationId      of the request to the media driver.
     */
    public Image(
        final int sessionId,
        final long initialPosition,
        final Position subscriberPosition,
        final LogBuffers logBuffers,
        final ErrorHandler errorHandler,
        final long correlationId)
    {
        this.correlationId = correlationId;
        this.sessionId = sessionId;
        this.subscriberPosition = subscriberPosition;
        this.logBuffers = logBuffers;
        this.errorHandler = errorHandler;

        final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
        termBuffers = Arrays.copyOf(buffers, PARTITION_COUNT);

        final int capacity = termBuffers[0].capacity();
        this.termLengthMask = capacity - 1;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(capacity);
        final int initialTermId = LogBufferDescriptor.initialTermId(buffers[LOG_META_DATA_SECTION_INDEX]);
        header = new Header(initialTermId, capacity);

        subscriberPosition.setOrdered(initialPosition);
    }

    /**
     * The sessionId for the steam of messages.
     *
     * @return the sessionId for the steam of messages.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * The initial term at which the stream started for this session.
     *
     * @return the initial term id.
     */
    public int initialTermId()
    {
        return header.initialTermId();
    }

    /**
     * The correlationId for identification of the image with the media driver.
     *
     * @return the correlationId for identification of the image with the media driver.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * The position this {@link Image} has been consumed to by the subscriber.
     *
     * @return the position this {@link Image} has been consumed to by the subscriber.
     */
    public long position()
    {
        return subscriberPosition.get();
    }

    /**
     * The {@link FileChannel} to the raw log of the Image.
     *
     * @return the {@link FileChannel} to the raw log of the Image.
     */
    public FileChannel fileChannel()
    {
        return logBuffers.fileChannel();
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the {@link FragmentHandler} up to a limited number of fragments as specified.
     *
     * @param fragmentHandler to which messages are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        final long position = subscriberPosition.get();
        final int termOffset = (int)position & termLengthMask;
        final UnsafeBuffer termBuffer = termBuffers[indexByPosition(position, positionBitsToShift)];

        final long readOutcome = read(termBuffer, termOffset, fragmentHandler, fragmentLimit, header, errorHandler);

        final long newPosition = position + (offset(readOutcome) - termOffset);
        if (newPosition > position)
        {
            subscriberPosition.setOrdered(newPosition);
        }

        return fragmentsRead(readOutcome);
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the {@link BlockHandler} up to a limited number of bytes.
     *
     * @param blockHandler     to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     */
    public int blockPoll(final BlockHandler blockHandler, final int blockLengthLimit)
    {
        final long position = subscriberPosition.get();
        final int termOffset = (int)position & termLengthMask;
        final UnsafeBuffer termBuffer = termBuffers[indexByPosition(position, positionBitsToShift)];
        final int limit = Math.min(termOffset + blockLengthLimit, termBuffer.capacity());

        final int resultingOffset = TermBlockScanner.scan(termBuffer, termOffset, limit);

        final int bytesConsumed = resultingOffset - termOffset;
        if (resultingOffset > termOffset)
        {
            try
            {
                final int termId = termId(termBuffer, termOffset);

                blockHandler.onBlock(termBuffer, termOffset, bytesConsumed, sessionId, termId);
            }
            catch (final Throwable t)
            {
                errorHandler.onError(t);
            }

            subscriberPosition.setOrdered(position + bytesConsumed);
        }

        return bytesConsumed;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the {@link FileBlockHandler} up to a limited number of bytes.
     *
     * @param fileBlockHandler to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     */
    public int filePoll(final FileBlockHandler fileBlockHandler, final int blockLengthLimit)
    {
        final long position = subscriberPosition.get();
        final int termOffset = (int)position & termLengthMask;
        final int activeIndex = indexByPosition(position, positionBitsToShift);
        final UnsafeBuffer termBuffer = termBuffers[activeIndex];
        final int capacity = termBuffer.capacity();
        final int limit = Math.min(termOffset + blockLengthLimit, capacity);

        final int resultingOffset = TermBlockScanner.scan(termBuffer, termOffset, limit);

        final int bytesConsumed = resultingOffset - termOffset;
        if (resultingOffset > termOffset)
        {
            try
            {
                final long offset = ((long)capacity * activeIndex) + termOffset;
                final int termId = termId(termBuffer, termOffset);

                fileBlockHandler.onBlock(logBuffers.fileChannel(), offset, bytesConsumed, sessionId, termId);
            }
            catch (final Throwable t)
            {
                errorHandler.onError(t);
            }

            subscriberPosition.setOrdered(position + bytesConsumed);
        }

        return bytesConsumed;
    }

    ManagedResource managedResource()
    {
        return new ImageManagedResource();
    }

    private static int termId(final UnsafeBuffer buffer, final int frameOffset)
    {
        return buffer.getInt(frameOffset + TERM_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    private class ImageManagedResource implements ManagedResource
    {
        private long timeOfLastStateChange = 0;

        public void timeOfLastStateChange(final long time)
        {
            this.timeOfLastStateChange = time;
        }

        public long timeOfLastStateChange()
        {
            return timeOfLastStateChange;
        }

        public void delete()
        {
            logBuffers.close();
        }
    }
}
