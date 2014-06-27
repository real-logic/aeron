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
import uk.co.real_logic.aeron.conductor.ClientConductorProxy;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.status.PositionIndicator;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.CLEAN_WINDOW;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.rotateId;
import static uk.co.real_logic.aeron.util.ChannelCounters.UNKNOWN_TERM_ID;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.TRIPPED;

/**
 * Aeron Channel
 */
public class Channel extends ChannelEndpoint implements AutoCloseable, PositionIndicator
{
    private final ClientConductorProxy clientConductorProxy;
    private final long sessionId;
    private final AtomicArray<Channel> channels;
    private final AtomicBoolean paused;

    private volatile LogAppender[] logAppenders;
    private PositionIndicator positionIndicator;

    private final AtomicLong currentTermId = new AtomicLong(UNKNOWN_TERM_ID);
    private final AtomicInteger currentBufferIndex = new AtomicInteger(0);

    public Channel(final String destination,
                   final ClientConductorProxy clientConductorProxy,
                   final long channelId,
                   final long sessionId,
                   final AtomicArray<Channel> channels,
                   final AtomicBoolean paused)
    {
        super(destination, channelId);

        this.clientConductorProxy = clientConductorProxy;
        this.sessionId = sessionId;
        this.channels = channels;
        this.paused = paused;
    }

    private boolean canAppend()
    {
        return logAppenders != null && !paused.get();
    }

    public void onBuffersMapped(final long termId,
                                final LogAppender[] logAppenders,
                                final PositionIndicator positionIndicator)
    {
        this.logAppenders = logAppenders;
        this.positionIndicator = positionIndicator;
        currentTermId.set(termId);
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
        if (!canAppend())
        {
            return false;
        }

        // TODO: must update the logAppender header with new termId!
        final int bufferIndex = currentBufferIndex.get();
        final LogAppender logAppender = logAppenders[bufferIndex];
        final AppendStatus status = logAppender.append(buffer, offset, length);

        if (status == TRIPPED)
        {
            currentBufferIndex.lazySet(rotateId(bufferIndex));
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

    public void close() throws Exception
    {
        channels.remove(this);
        clientConductorProxy.removePublication(destination(), sessionId, channelId());
    }

    public boolean matches(final String destination, final long sessionId, final long channelId)
    {
        return destination.equals(this.destination()) && this.sessionId == sessionId && this.channelId() == channelId;
    }

    private void requestTerm(final long termId)
    {
        clientConductorProxy.sendRequestTerm(destination(), sessionId, channelId(), termId);
    }

    protected boolean hasTerm(final long sessionId)
    {
        return currentTermId.get() != UNKNOWN_TERM_ID;
    }

    protected void requestTermRoll(final long currentTermId)
    {
        requestTerm(currentTermId + CLEAN_WINDOW);
    }

    public boolean hasSessionId(final long sessionId)
    {
        return this.sessionId == sessionId;
    }

    public long position()
    {
        logAppenders[currentBufferIndex.get()].tailVolatile();

        return 0;
    }
}
