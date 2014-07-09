/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.status.LimitBarrier;

import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.TRIPPED;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * Publication end of a channel for publishing messages to subscribers.
 * <p>
 * Publication instances are threadsafe and can be shared between publishers.
 */
public class Publication
{
    public static final long NO_DIRTY_TERM = -1L;

    private final ClientConductor conductor;

    private final String destination;
    private final long channelId;
    private final long sessionId;
    private final LogAppender[] logAppenders;
    private final LimitBarrier limit;
    private final AtomicLong currentTermId;
    private final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();

    private volatile long dirtyTermId = NO_DIRTY_TERM;

    private int refCount = 0;
    private int currentBufferIndex = 0;

    public Publication(final ClientConductor conductor,
                       final String destination,
                       final long channelId,
                       final long sessionId,
                       final long initialTermId,
                       final LogAppender[] logAppenders,
                       final LimitBarrier limit)
    {
        this.conductor = conductor;

        this.destination = destination;
        this.channelId = channelId;
        this.sessionId = sessionId;
        this.currentTermId = new AtomicLong(initialTermId);
        this.logAppenders = logAppenders;
        this.limit = limit;
    }

    /**
     * Media address for delivery to the destination.
     *
     * @return Media address for delivery to the destination.
     */
    public String destination()
    {
        return destination;
    }

    /**
     * Channel identity for scoping within the destination media address.
     *
     * @return Channel identity for scoping within the destination media address.
     */
    public long channelId()
    {
        return channelId;
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return true if buffer is sent otherwise false.
     */
    public boolean offer(final AtomicBuffer buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    /**
     * Non-blocking publish of a partial buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return true if the message can be published otherwise false.
     */
    public boolean offer(final AtomicBuffer buffer, final int offset, final int length)
    {
        final LogAppender logAppender = logAppenders[currentBufferIndex];
        if (isPausedDueToFlowControl(logAppender, length))
        {
            return false;
        }

        final AppendStatus status = logAppender.append(buffer, offset, length);
        if (status == TRIPPED)
        {
            nextTerm();

            return offer(buffer, offset, length);
        }

        return status == SUCCESS;
    }

    private void nextTerm()
    {
        final int nextIndex = rotateNext(currentBufferIndex);

        final LogAppender nextAppender = logAppenders[nextIndex];
        if (CLEAN != nextAppender.status())
        {
            System.err.println(String.format("Term not clean: destination=%s channelId=%d, required termId=%d",
                                             destination, channelId, currentTermId.get() + 1));

            if (nextAppender.compareAndSetStatus(NEEDS_CLEANING, IN_CLEANING))
            {
                nextAppender.clean(); // Conductor is not keeping up so do it yourself!!!
            }
            else
            {
                while (CLEAN != nextAppender.status())
                {
                    Thread.yield();
                }
            }
        }

        final long currentTermId = this.currentTermId.get();
        final long newTermId = currentTermId + 1;

        dataHeaderFlyweight.wrap(nextAppender.defaultHeader());
        dataHeaderFlyweight.termId(newTermId);

        this.currentTermId.lazySet(newTermId);
        currentBufferIndex = nextIndex;

        final int previousIndex = rotatePrevious(currentBufferIndex);
        logAppenders[previousIndex].statusOrdered(NEEDS_CLEANING);
        requestTermClean(currentTermId);
    }

    private boolean isPausedDueToFlowControl(final LogAppender logAppender, final int length)
    {
        // TODO: need to take account of bytes in previous terms.
        final int requiredPosition = logAppender.tailVolatile() + length;
        return limit.limit() < requiredPosition;
    }

    /**
     * Release this reference to the {@link Publication}. To be called by the end using publisher.
     * If all references are released then the associated buffers can be released.
     */
    public void release()
    {
        synchronized (conductor)
        {
            if (--refCount == 0)
            {
                conductor.releasePublication(this);
            }
        }
    }

    /**
     * Accessed by the client conductor.
     */
    public void incRef()
    {
        synchronized (conductor)
        {
            ++refCount;
        }
    }

    private void requestTermClean(final long currentTermId)
    {
        dirtyTermId = currentTermId + CLEAN_WINDOW;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public long dirtyTermId()
    {
        return dirtyTermId;
    }
}
