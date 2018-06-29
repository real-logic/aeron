/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron;

import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.*;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.Position;

import java.nio.channels.FileChannel;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.endOfStreamPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.indexByPosition;
import static io.aeron.logbuffer.TermReader.read;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.TERM_ID_FIELD_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Represents a replicated publication {@link Image} from a publisher to a {@link Subscription}.
 * Each {@link Image} identifies a source publisher by session id.
 * <p>
 * By default fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether or not they were fragmented, then the Subscriber
 * should be created with a {@link FragmentAssembler} or a custom implementation.
 * <p>
 * It is an application's responsibility to {@link #poll} the {@link Image} for new messages.
 * <p>
 * <b>Note:</b>Images are not threadsafe and should not be shared between subscribers.
 */
public class Image
{
    private final long correlationId;
    private final long joinPosition;
    private long finalPosition;
    private final int sessionId;
    private final int initialTermId;
    private final int termLengthMask;
    private final int positionBitsToShift;
    private boolean isEos;
    private volatile boolean isClosed;

    private final Position subscriberPosition;
    private final UnsafeBuffer[] termBuffers;
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
        this.joinPosition = subscriberPosition.get();

        termBuffers = logBuffers.duplicateTermBuffers();

        final int termLength = logBuffers.termLength();
        this.termLengthMask = termLength - 1;
        this.positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        this.initialTermId = LogBufferDescriptor.initialTermId(logBuffers.metaDataBuffer());
        header = new Header(initialTermId, positionBitsToShift, this);
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @return the length in bytes for each term partition in the log buffer.
     */
    public int termBufferLength()
    {
        return termLengthMask + 1;
    }

    /**
     * The sessionId for the steam of messages. Sessions are unique within a {@link Subscription} and unique across
     * all {@link Publication}s from a {@link #sourceIdentity()}.
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
     * The length in bytes of the MTU (Maximum Transmission Unit) the Sender used for the datagram.
     *
     * @return length in bytes of the MTU (Maximum Transmission Unit) the Sender used for the datagram.
     */
    public int mtuLength()
    {
        return LogBufferDescriptor.mtuLength(logBuffers.metaDataBuffer());
    }

    /**
     * The initial term at which the stream started for this session.
     *
     * @return the initial term id.
     */
    public int initialTermId()
    {
        return initialTermId;
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
     * Get the position the subscriber joined this stream at.
     *
     * @return the position the subscriber joined this stream at.
     */
    public long joinPosition()
    {
        return joinPosition;
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
            return finalPosition;
        }

        return subscriberPosition.get();
    }

    /**
     * Set the subscriber position for this {@link Image} to indicate where it has been consumed to.
     *
     * @param newPosition for the consumption point.
     */
    public void position(final long newPosition)
    {
        if (isClosed)
        {
            throw new AeronException("Image is closed");
        }

        validatePosition(newPosition);

        subscriberPosition.setOrdered(newPosition);
    }

    /**
     * The counter id for the subscriber position counter.
     *
     * @return the id for the subscriber position counter.
     */
    public int subscriberPositionId()
    {
        return subscriberPosition.id();
    }

    /**
     * Is the current consumed position at the end of the stream?
     *
     * @return true if at the end of the stream or false if not.
     */
    public boolean isEndOfStream()
    {
        if (isClosed)
        {
            return isEos;
        }

        return subscriberPosition.get() >= endOfStreamPosition(logBuffers.metaDataBuffer());
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
     * <p>
     * Use a {@link FragmentAssembler} to assemble messages which span multiple fragments.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see FragmentAssembler
     * @see ImageFragmentAssembler
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        final long position = subscriberPosition.get();

        return read(
            activeTermBuffer(position),
            (int)position & termLengthMask,
            fragmentHandler,
            fragmentLimit,
            header,
            errorHandler,
            position,
            subscriberPosition);
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link ControlledFragmentHandler} up to a limited number of fragments as specified.
     * <p>
     * Use a {@link ControlledFragmentAssembler} to assemble messages which span multiple fragments.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see ControlledFragmentAssembler
     * @see ImageControlledFragmentAssembler
     */
    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        int fragmentsRead = 0;
        long initialPosition = subscriberPosition.get();
        int initialOffset = (int)initialPosition & termLengthMask;
        int resultingOffset = initialOffset;
        final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);
        final int capacity = termBuffer.capacity();
        header.buffer(termBuffer);

        try
        {
            do
            {
                final int length = frameLengthVolatile(termBuffer, resultingOffset);
                if (length <= 0)
                {
                    break;
                }

                final int frameOffset = resultingOffset;
                final int alignedLength = BitUtil.align(length, FRAME_ALIGNMENT);
                resultingOffset += alignedLength;

                if (isPaddingFrame(termBuffer, frameOffset))
                {
                    continue;
                }

                header.offset(frameOffset);

                final Action action = fragmentHandler.onFragment(
                    termBuffer,
                    frameOffset + HEADER_LENGTH,
                    length - HEADER_LENGTH,
                    header);

                if (action == ABORT)
                {
                    resultingOffset -= alignedLength;
                    break;
                }

                ++fragmentsRead;

                if (action == BREAK)
                {
                    break;
                }
                else if (action == COMMIT)
                {
                    initialPosition += (resultingOffset - initialOffset);
                    initialOffset = resultingOffset;
                    subscriberPosition.setOrdered(initialPosition);
                }
            }
            while (fragmentsRead < fragmentLimit && resultingOffset < capacity);
        }
        catch (final Throwable t)
        {
            errorHandler.onError(t);
        }
        finally
        {
            final long resultingPosition = initialPosition + (resultingOffset - initialOffset);
            if (resultingPosition > initialPosition)
            {
                subscriberPosition.setOrdered(resultingPosition);
            }
        }

        return fragmentsRead;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link ControlledFragmentHandler} up to a limited number of fragments as specified or
     * the maximum position specified.
     * <p>
     * Use a {@link ControlledFragmentAssembler} to assemble messages which span multiple fragments.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param maxPosition     to consume messages up to.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see ControlledFragmentAssembler
     * @see ImageControlledFragmentAssembler
     */
    public int boundedControlledPoll(
        final ControlledFragmentHandler fragmentHandler, final long maxPosition, final int fragmentLimit)
    {
        if (isClosed)
        {
            return 0;
        }

        int fragmentsRead = 0;
        long initialPosition = subscriberPosition.get();
        int initialOffset = (int)initialPosition & termLengthMask;
        int resultingOffset = initialOffset;
        final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);
        final int endOffset = (int)Math.min(termBuffer.capacity(), maxPosition - initialPosition + initialOffset);
        header.buffer(termBuffer);

        try
        {
            while (fragmentsRead < fragmentLimit && resultingOffset < endOffset)
            {
                final int length = frameLengthVolatile(termBuffer, resultingOffset);
                if (length <= 0)
                {
                    break;
                }

                final int frameOffset = resultingOffset;
                final int alignedLength = BitUtil.align(length, FRAME_ALIGNMENT);
                resultingOffset += alignedLength;

                if (isPaddingFrame(termBuffer, frameOffset))
                {
                    continue;
                }

                header.offset(frameOffset);

                final Action action = fragmentHandler.onFragment(
                    termBuffer,
                    frameOffset + HEADER_LENGTH,
                    length - HEADER_LENGTH,
                    header);

                if (action == ABORT)
                {
                    resultingOffset -= alignedLength;
                    break;
                }

                ++fragmentsRead;

                if (action == BREAK)
                {
                    break;
                }
                else if (action == COMMIT)
                {
                    initialPosition += (resultingOffset - initialOffset);
                    initialOffset = resultingOffset;
                    subscriberPosition.setOrdered(initialPosition);
                }
            }
        }
        catch (final Throwable t)
        {
            errorHandler.onError(t);
        }
        finally
        {
            final long resultingPosition = initialPosition + (resultingOffset - initialOffset);
            if (resultingPosition > initialPosition)
            {
                subscriberPosition.setOrdered(resultingPosition);
            }
        }

        return fragmentsRead;
    }

    /**
     * Peek for new messages in a stream by scanning forward from an initial position. If new messages are found then
     * they will be delivered to the {@link ControlledFragmentHandler} up to a limited position.
     * <p>
     * Use a {@link ControlledFragmentAssembler} to assemble messages which span multiple fragments. Scans must also
     * start at the beginning of a message so that the assembler is reset.
     *
     * @param initialPosition from which to peek forward.
     * @param fragmentHandler to which message fragments are delivered.
     * @param limitPosition   up to which can be scanned.
     * @return the resulting position after the scan terminates which is a complete message.
     * @see ControlledFragmentAssembler
     * @see ImageControlledFragmentAssembler
     */
    public long controlledPeek(
        final long initialPosition, final ControlledFragmentHandler fragmentHandler, final long limitPosition)
    {
        if (isClosed)
        {
            return 0;
        }

        validatePosition(initialPosition);

        int initialOffset = (int)initialPosition & termLengthMask;
        int offset = initialOffset;
        long position = initialPosition;
        final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);
        final int capacity = termBuffer.capacity();
        header.buffer(termBuffer);
        long resultingPosition = initialPosition;

        try
        {
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

                if (isPaddingFrame(termBuffer, frameOffset))
                {
                    continue;
                }

                header.offset(frameOffset);

                final Action action = fragmentHandler.onFragment(
                    termBuffer,
                    frameOffset + HEADER_LENGTH,
                    length - HEADER_LENGTH,
                    header);

                if (action == ABORT)
                {
                    break;
                }

                position += (offset - initialOffset);
                initialOffset = offset;

                if ((header.flags() & END_FRAG_FLAG) == END_FRAG_FLAG)
                {
                    resultingPosition = position;
                }

                if (action == BREAK)
                {
                    break;
                }
            }
            while (position < limitPosition && offset < capacity);
        }
        catch (final Throwable t)
        {
            errorHandler.onError(t);
        }

        return resultingPosition;
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
            finally
            {
                subscriberPosition.setOrdered(position + bytesConsumed);
            }
        }

        return bytesConsumed;
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the {@link RawBlockHandler} up to a limited number of bytes.
     * <p>
     * This method is useful for operations like bulk archiving a stream to file.
     *
     * @param rawBlockHandler  to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     */
    public int rawPoll(final RawBlockHandler rawBlockHandler, final int blockLengthLimit)
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
        final int length = resultingOffset - termOffset;

        if (resultingOffset > termOffset)
        {
            try
            {
                final long fileOffset = ((long)capacity * activeIndex) + termOffset;
                final int termId = termBuffer.getInt(termOffset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);

                rawBlockHandler.onBlock(
                    logBuffers.fileChannel(), fileOffset, termBuffer, termOffset, length, sessionId, termId);
            }
            catch (final Throwable t)
            {
                errorHandler.onError(t);
            }
            finally
            {
                subscriberPosition.setOrdered(position + length);
            }
        }

        return length;
    }

    private UnsafeBuffer activeTermBuffer(final long position)
    {
        return termBuffers[indexByPosition(position, positionBitsToShift)];
    }

    private void validatePosition(final long newPosition)
    {
        final long currentPosition = subscriberPosition.get();
        final long limitPosition = currentPosition + termBufferLength();
        if (newPosition < currentPosition || newPosition > limitPosition)
        {
            throw new IllegalArgumentException(
                "newPosition of " + newPosition + " out of range " + currentPosition + "-" + limitPosition);
        }

        if (0 != (newPosition & (FRAME_ALIGNMENT - 1)))
        {
            throw new IllegalArgumentException("newPosition of " + newPosition + " not aligned to FRAME_ALIGNMENT");
        }
    }

    LogBuffers logBuffers()
    {
        return logBuffers;
    }

    void close()
    {
        finalPosition = subscriberPosition.getVolatile();
        isEos = finalPosition >= endOfStreamPosition(logBuffers.metaDataBuffer());
        isClosed = true;
    }
}
