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

import uk.co.real_logic.aeron.conductor.ChannelNotifiable;
import uk.co.real_logic.aeron.conductor.MediaConductorProxy;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.status.PositionIndicator;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.CLEAN_WINDOW;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.rotateId;
import static uk.co.real_logic.aeron.util.ChannelCounters.UNKNOWN_TERM_ID;

/**
 * Aeron Channel
 */
public class Channel extends ChannelNotifiable implements AutoCloseable, PositionIndicator
{
    private final MediaConductorProxy mediaConductorProxy;
    private final long sessionId;
    private final AtomicArray<Channel> channels;
    private final AtomicBoolean paused;

    private volatile LogAppender[] logAppenders;
    private PositionIndicator positionIndicator;

    private final AtomicLong currentTermId = new AtomicLong(UNKNOWN_TERM_ID);
    private final AtomicLong cleanedTermId = new AtomicLong(UNKNOWN_TERM_ID);

    private int currentBufferId = 0;

    public Channel(final String destination,
                   final MediaConductorProxy mediaConductorProxy,
                   final long channelId,
                   final long sessionId,
                   final AtomicArray<Channel> channels,
                   final AtomicBoolean paused)
    {
        super(destination, channelId);

        this.mediaConductorProxy = mediaConductorProxy;
        this.sessionId = sessionId;
        this.channels = channels;
        this.paused = paused;
    }

    public long channelId()
    {
        return channelId;
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
        cleanedTermId.set(termId + CLEAN_WINDOW);
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

        final LogAppender logAppender = logAppenders[currentBufferId];
        final boolean hasAppended = logAppender.append(buffer, offset, length);
        if (!hasAppended && currentTermId.get() <= cleanedTermId.get())
        {
            requestTermRoll();
            currentBufferId = rotateId(currentBufferId);
            currentTermId.incrementAndGet();
        }

        return hasAppended;
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
        mediaConductorProxy.sendRemoveChannel(destination, sessionId, channelId);
    }

    public boolean matches(final String destination, final long sessionId, final long channelId)
    {
        return destination.equals(this.destination) && this.sessionId == sessionId && this.channelId == channelId;
    }

    private void requestTerm(final long termId)
    {
        mediaConductorProxy.sendRequestTerm(destination, sessionId, channelId, termId);
    }

    protected boolean hasTerm(final long sessionId)
    {
        return currentTermId.get() != UNKNOWN_TERM_ID;
    }

    protected void requestTermRoll()
    {
        requestTerm(currentTermId.get() + CLEAN_WINDOW);
    }

    public boolean hasSessionId(final long sessionId)
    {
        return this.sessionId == sessionId;
    }

    /**
     * This is performed on the Client Conductor's thread
     */
    public void processBufferScan()
    {
        long currentTermId = this.currentTermId.get();
        if (currentTermId == UNKNOWN_TERM_ID)
        {
            // Doesn't have any buffers yet
            return;
        }

        final long requiredCleanTermid = currentTermId + 1;
        if (requiredCleanTermid > cleanedTermId.get())
        {
            LogAppender requiredBuffer = logAppenders[rotateId(currentBufferId)];
            if (hasBeenCleaned(requiredBuffer))
            {
                cleanedTermId.incrementAndGet();
            }
        }
    }

    private boolean hasBeenCleaned(final LogAppender appender)
    {
        return appender.tailVolatile() == 0;
    }

    public long position()
    {
        logAppenders[currentBufferId].tailVolatile();
        return 0;
    }
}
