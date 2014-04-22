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

import uk.co.real_logic.aeron.admin.ChannelNotifiable;
import uk.co.real_logic.aeron.admin.ClientAdminThreadCursor;
import uk.co.real_logic.aeron.admin.TermBufferNotifier;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron Channel
 */
public class Channel extends ChannelNotifiable implements AutoCloseable
{
    private final ClientAdminThreadCursor adminThread;
    private final long sessionId;
    private final AtomicArray<Channel> channels;
    private final AtomicBoolean paused;

    private LogAppender[] logAppenders;

    public Channel(final String destination,
                   final ClientAdminThreadCursor adminCursor,
                   final TermBufferNotifier bufferNotifier,
                   final long channelId,
                   final long sessionId,
                   final AtomicArray<Channel> channels,
                   final AtomicBoolean paused)
    {
        super(bufferNotifier, destination, channelId);
        this.adminThread = adminCursor;
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

    public void onBuffersMapped(final LogAppender[] logAppenders)
    {
        this.logAppenders = logAppenders;
    }

    /**
     * Non blocking message send
     *
     * @param buffer
     * @return
     */
    public boolean offer(final AtomicBuffer buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    public boolean offer(final AtomicBuffer buffer, final int offset, final int length)
    {
        if (!canAppend())
        {
            return false;
        }

        final LogAppender logAppender = logAppenders[currentBuffer];
        boolean hasAppended = logAppender.append(buffer, offset, length);
        if (!hasAppended)
        {
            next();
        }

        return hasAppended;
    }

    public void send(final AtomicBuffer buffer) throws BufferExhaustedException
    {
        send(buffer, 0, buffer.capacity());
    }

    public void send(final AtomicBuffer buffer, final int offset, final int length) throws BufferExhaustedException
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
        adminThread.sendRemoveChannel(destination, sessionId, channelId);
    }

    public boolean matches(final String destination, final long sessionId, final long channelId)
    {
        return destination.equals(this.destination) && this.sessionId == sessionId && this.channelId == channelId;
    }

    private void requestTerm(final long termId)
    {
        adminThread.sendRequestTerm(destination, sessionId, channelId, termId);
    }

    protected void rollTerm()
    {
        requestTerm(currentTermId.incrementAndGet() + 1);
        startTerm();
    }

    public boolean hasSessionId(final long sessionId)
    {
        return this.sessionId == sessionId;
    }

    public void initialTerm(final long sessionId, final long termId)
    {
        currentTermId.set(termId);
    }

    public void cleanedTermBuffer(final long sessionId, final long termId)
    {
        cleanedTermId.set(termId);
    }
}
