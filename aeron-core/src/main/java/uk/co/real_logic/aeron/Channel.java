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

import java.nio.ByteBuffer;

/**
 * Aeron Channel
 */
public class Channel extends ChannelNotifiable implements AutoCloseable
{
    private final ClientAdminThreadCursor adminThread;
    private final long sessionId;
    private final AtomicArray<Channel> channels;

    public Channel(final String destination,
                   final ClientAdminThreadCursor adminCursor,
                   final TermBufferNotifier bufferNotifier,
                   final long channelId,
                   final long sessionId,
                   final AtomicArray<Channel> channels)
    {
        super(bufferNotifier, destination, channelId);
        this.adminThread = adminCursor;
        this.sessionId = sessionId;
        this.channels = channels;
    }

    public long channelId()
    {
        return channelId;
    }

    /**
     * Non blocking message send
     *
     * @param buffer
     * @return
     */
    public boolean offer(final ByteBuffer buffer)
    {
        return offer(buffer, buffer.position(), buffer.remaining());
    }

    public boolean offer(final ByteBuffer buffer, final int offset, final int length)
    {
        if (!hasTerm())
        {
            return false;
        }

        // TODO
        return true;
    }

    public void send(final ByteBuffer buffer) throws BufferExhaustedException
    {
        send(buffer, buffer.position(), buffer.remaining());
    }

    public void send(final ByteBuffer buffer, final int offset, final int length) throws BufferExhaustedException
    {
        if (!hasTerm())
        {
            throw new BufferExhaustedException("Unable to send: awaiting buffer creation");
        }
        // TODO
    }

    public void blockingSend(final ByteBuffer buffer)
    {
        blockingSend(buffer, buffer.position(), buffer.remaining());
    }

    public void blockingSend(final ByteBuffer buffer, final int offset, final int length)
    {
        // TODO: Is this necessary?
    }

    public void close() throws Exception
    {
        channels.remove(this);
        adminThread.sendRemoveChannel(destination, channelId);
    }

    public boolean matches(final String destination, final long sessionId, final long channelId)
    {
        return destination.equals(this.destination) && this.sessionId == sessionId && this.channelId == channelId;
    }

    private void requestTerm(final long termId)
    {
        adminThread.sendRequestTerm(channelId, termId, destination);
    }

    private void rollTerm()
    {
        bufferNotifier.endOfTermBuffer(currentTermId.get());
        currentTermId.incrementAndGet();
        requestTerm(currentTermId.get() + 1);
        startTerm();
    }

    public boolean hasSessionId(final long sessionId)
    {
        return this.sessionId == sessionId;
    }
}
