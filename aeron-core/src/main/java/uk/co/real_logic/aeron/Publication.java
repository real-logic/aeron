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

import static uk.co.real_logic.aeron.util.TermHelper.rotateNext;
import static uk.co.real_logic.aeron.util.TermHelper.rotatePrevious;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.TRIPPED;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * Publication end of a channel for publishing messages to subscribers.
 * <p>
 * Note: Publication instances are threadsafe and can be shared between publisher threads.
 */
public class Publication
{
    private final ClientConductor conductor;
    private final String destination;
    private final long channelId;
    private final long sessionId;
    private final LogAppender[] logAppenders;
    private final LimitBarrier limitBarrier;
    private final AtomicLong activeTermId;
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private int refCount = 0;
    private int activeIndex = 0;

    public Publication(final ClientConductor conductor,
                       final String destination,
                       final long channelId,
                       final long sessionId,
                       final long initialTermId,
                       final LogAppender[] logAppenders,
                       final LimitBarrier limitBarrier)
    {
        this.conductor = conductor;

        this.destination = destination;
        this.channelId = channelId;
        this.sessionId = sessionId;
        this.activeTermId = new AtomicLong(initialTermId);
        this.logAppenders = logAppenders;
        this.limitBarrier = limitBarrier;
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
     * Session under which messages are published.
     *
     * @return the session id for this publication.
     */
    public long sessionId()
    {
        return sessionId;
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
        final LogAppender logAppender = logAppenders[activeIndex];
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
        final int nextIndex = rotateNext(activeIndex);

        final LogAppender nextAppender = logAppenders[nextIndex];
        if (CLEAN != nextAppender.status())
        {
            System.err.println(String.format("Term not clean: destination=%s channelId=%d, required termId=%d",
                                             destination, channelId, activeTermId.get() + 1));

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

        final long activeTermId = this.activeTermId.get();
        final long newTermId = activeTermId + 1;

        dataHeader.wrap(nextAppender.defaultHeader());
        dataHeader.termId(newTermId);

        this.activeTermId.lazySet(newTermId);
        final int previousIndex = rotatePrevious(activeIndex);
        activeIndex = nextIndex;
        logAppenders[previousIndex].statusOrdered(NEEDS_CLEANING);
    }

    private boolean isPausedDueToFlowControl(final LogAppender logAppender, final int length)
    {
        // TODO: need to take account of bytes in previous terms.
        final int requiredPosition = logAppender.tailVolatile() + length;
        return limitBarrier.limit() < requiredPosition;
    }
}
