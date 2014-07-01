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

import uk.co.real_logic.aeron.conductor.ChannelEndpoint;
import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.status.PositionIndicator;

import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.CLEAN_WINDOW;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.rotateId;
import static uk.co.real_logic.aeron.util.ChannelCounters.UNKNOWN_TERM_ID;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.TRIPPED;

/**
 * Aeron Publication
 */
public class Publication extends ChannelEndpoint implements PositionIndicator
{
    public static final long NO_DIRTY_TERM = -1L;

    private final ClientConductor conductor;

    private final long sessionId;
    private final LogAppender[] logAppenders;
    private final PositionIndicator positionIndicator;
    private final AtomicLong currentTermId;
    private volatile long dirtyTermId = NO_DIRTY_TERM;

    private int currentBufferIndex = 0;
    public Publication(final ClientConductor conductor,
                       final String destination,
                       final long channelId,
                       final long sessionId,
                       final long initialTermId,
                       final LogAppender[] logAppenders,
                       final PositionIndicator positionIndicator)
    {
        super(destination, channelId);
        this.conductor = conductor;

        currentTermId = new AtomicLong(initialTermId);
        this.sessionId = sessionId;
        this.logAppenders = logAppenders;
        this.positionIndicator = positionIndicator;
    }

    /**
     * Non blocking message send
     *
     * @param buffer containing message.
     * @return true if buffer is sent otherwise false.
     */
    public boolean offer(final AtomicBuffer buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    /**
     * Non-blocking send of a partial buffer.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return true if he message can be sent otherwise false.
     */
    public boolean offer(final AtomicBuffer buffer, final int offset, final int length)
    {
        // TODO: must update the logAppender header with new termId!
        final LogAppender logAppender = logAppenders[currentBufferIndex];
        final AppendStatus status = logAppender.append(buffer, offset, length);

        if (status == TRIPPED)
        {
            currentBufferIndex = rotateId(currentBufferIndex);
            final long currentTermId = this.currentTermId.get();
            this.currentTermId.lazySet(currentTermId + 1);

            requestTermRoll(currentTermId);

            return offer(buffer, offset, length);
        }

        return status == SUCCESS;
    }

    public void send(final AtomicBuffer buffer) throws BufferExhaustedException
    {
        send(buffer, 0, buffer.capacity());
    }

    public void send(final AtomicBuffer buffer, final int offset, final int length)
        throws BufferExhaustedException
    {
        if (!offer(buffer, offset, length))
        {
            bufferExhausted();
        }
    }

    private void bufferExhausted() throws BufferExhaustedException
    {
        throw new BufferExhaustedException("Unable to send: no space in buffer");
    }

    public void release() throws Exception
    {
        conductor.release(this);
    }

    public boolean matches(final String destination, final long sessionId, final long channelId)
    {
        return destination.equals(this.destination()) && this.sessionId == sessionId && this.channelId() == channelId;
    }

    private void requestTerm(final long termId)
    {
        // TODO
    }

    protected boolean hasTerm(final long sessionId)
    {
        return currentTermId.get() != UNKNOWN_TERM_ID;
    }

    protected void requestTermRoll(final long currentTermId)
    {
        requestTerm(currentTermId + CLEAN_WINDOW);
    }

    public long position()
    {
        logAppenders[currentBufferIndex].tailVolatile();

        return 0;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public long dirtyTermId()
    {
        return dirtyTermId;
    }

    public void resetDirtyTermId()
    {
        dirtyTermId = NO_DIRTY_TERM;
    }
}
