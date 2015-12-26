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
import uk.co.real_logic.aeron.logbuffer.ControlledFragmentHandler.Action;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.ManagedResource;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;

import java.nio.channels.FileChannel;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameLengthVolatile;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.isPaddingFrame;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.logbuffer.TermReader.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
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
    private volatile boolean isClosed;

    private final Position subscriberPosition;
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final Header header;
    private final ErrorHandler errorHandler;
    private final LogBuffers logBuffers;
    private final String sourceIdentity;
    private final Subscription subscription;

    /**
     * Construct a new image over a log to represent a stream of messages from a {@link Publication}.
     *
     * @param subscription       to which this {@link Image} belongs.
     * @param sessionId          of the stream of messages.
     * @param subscriberPosition for indicating the position of the subscriber in the stream.
     * @param logBuffers         containing the stream of messages.
     * @param errorHandler       to be called if an error occurs when polling for messages.
     * @param sourceIdentity     of the source sending the stream of messages.
     * @param correlationId      of the request to the media driver.
     */
    public Image(
        final Subscription subscription,
        final int sessionId,
        final Position subscriberPosition,
        final LogBuffers logBuffers,
        final ErrorHandler errorHandler,
        final String sourceIdentity,
        final long correlationId)
    {
        this.subscription = subscription;
        this.sessionId = sessionId;
        this.subscriberPosition = subscriberPosition;
        this.logBuffers = logBuffers;
        this.errorHandler = errorHandler;
        this.sourceIdentity = sourceIdentity;
        this.correlationId = correlationId;

        final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
        System.arraycopy(buffers, 0, termBuffers, 0, PARTITION_COUNT);

        final int termLength = logBuffers.termLength();
        this.termLengthMask = termLength - 1;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        header = new Header(LogBufferDescriptor.initialTermId(buffers[LOG_META_DATA_SECTION_INDEX]), termLength);
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @return the length in bytes for each term partition in the log buffer.
     */
    public int termBufferLength()
    {
        return logBuffers.termLength();
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
     * The source identity of the sending publisher as an abstract concept appropriate for the media.
     *
     * @return source identity of the sending publisher as an abstract concept appropriate for the media.
     */
    public String sourceIdentity()
    {
        return sourceIdentity;
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
     * Get the {@link Subscription} to which this {@link Image} belongs.
     *
     * @return the {@link Subscription} to which this {@link Image} belongs.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * The position this {@link Image} has been consumed to by the subscriber.
     *
     * @return the position this {@link Image} has been consumed to by the subscriber.
     */
    public long position()
    {
        if (isClosed)
        {
            return 0;
        }

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
     * will be delivered to the {@link FragmentHandler} up to a limited number of fragments as specified.
     *
     * To assemble messages that span multiple fragments then use {@link FragmentAssembler}.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see FragmentAssembler
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        final long position = subscriberPosition.get();
        final int termOffset = (int)position & termLengthMask;
        final UnsafeBuffer termBuffer = activeTermBuffer(position);

        final long outcome = read(termBuffer, termOffset, fragmentHandler, fragmentLimit, header, errorHandler);

        updatePosition(position, termOffset, offset(outcome));

        return fragmentsRead(outcome);
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link ControlledFragmentHandler} up to a limited number of fragments as specified.
     *
     * To assemble messages that span multiple fragments then use {@link ControlledFragmentAssembler}.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see ControlledFragmentAssembler
     */
    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        long position = subscriberPosition.get();
        int termOffset = (int)position & termLengthMask;
        int offset = termOffset;
        int fragmentsRead = 0;
        final UnsafeBuffer termBuffer = activeTermBuffer(position);

        try
        {
            final int capacity = termBuffer.capacity();
            do
            {
                final int length = frameLengthVolatile(termBuffer, offset);
                if (length <= 0)
                {
                    break;
                }

                final int frameOffset = offset;
                final int alignedLength = BitUtil.align(length, FRAME_ALIGNMENT);
                offset += alignedLength;

                if (!isPaddingFrame(termBuffer, frameOffset))
                {
                    header.buffer(termBuffer);
                    header.offset(frameOffset);

                    final Action action = fragmentHandler.onFragment(
                        termBuffer, frameOffset + HEADER_LENGTH, length - HEADER_LENGTH, header);

                    ++fragmentsRead;

                    if (action == BREAK)
                    {
                        break;
                    }
                    else if (action == ABORT)
                    {
                        --fragmentsRead;
                        offset = frameOffset;
                        break;
                    }
                    else if (action == COMMIT)
                    {
                        position += alignedLength;
                        termOffset = offset;
                        subscriberPosition.setOrdered(position);
                    }
                }
            }
            while (fragmentsRead < fragmentLimit && offset < capacity);
        }
        catch (final Throwable t)
        {
            errorHandler.onError(t);
        }

        updatePosition(position, termOffset, offset);

        return fragmentsRead;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link BlockHandler} up to a limited number of bytes.
     *
     * @param blockHandler     to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     */
    public int blockPoll(final BlockHandler blockHandler, final int blockLengthLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        final long position = subscriberPosition.get();
        final int termOffset = (int)position & termLengthMask;
        final UnsafeBuffer termBuffer = activeTermBuffer(position);
        final int limit = Math.min(termOffset + blockLengthLimit, termBuffer.capacity());

        final int resultingOffset = TermBlockScanner.scan(termBuffer, termOffset, limit);

        final int bytesConsumed = resultingOffset - termOffset;
        if (resultingOffset > termOffset)
        {
            try
            {
                final int termId = termBuffer.getInt(termOffset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);

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
     * will be delivered to the {@link FileBlockHandler} up to a limited number of bytes.
     *
     * @param fileBlockHandler to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     */
    public int filePoll(final FileBlockHandler fileBlockHandler, final int blockLengthLimit)
    {
        if (isClosed)
        {
            return 0;
        }

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
                final int termId = termBuffer.getInt(termOffset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);

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

    private void updatePosition(final long positionBefore, final int offsetBefore, final int offsetAfter)
    {
        final long position = positionBefore + (offsetAfter - offsetBefore);
        if (position > positionBefore)
        {
            subscriberPosition.setOrdered(position);
        }
    }

    private UnsafeBuffer activeTermBuffer(final long position)
    {
        return termBuffers[indexByPosition(position, positionBitsToShift)];
    }

    ManagedResource managedResource()
    {
        isClosed = true;
        return new ImageManagedResource();
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
