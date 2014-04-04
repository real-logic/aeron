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

import uk.co.real_logic.aeron.admin.ClientAdminThreadCursor;
import uk.co.real_logic.aeron.admin.TermBufferNotifier;
import uk.co.real_logic.aeron.util.AtomicArray;

import java.nio.ByteBuffer;

/**
 * Aeron Channel
 */
public class Channel implements AutoCloseable
{
    private static final long UNKNOWN_TERM_ID = -1L;

    private final String destination;
    private final ClientAdminThreadCursor adminThread;
    private final TermBufferNotifier bufferNotifier;
    private final long channelId;
    private final long sessionId;
    private final AtomicArray<Channel> channels;

    private long currentTermId;

    public Channel(final String destination,
                   final ClientAdminThreadCursor adminCursor,
                   final TermBufferNotifier bufferNotifier,
                   final long channelId,
                   final long sessionId,
                   final AtomicArray<Channel> channels)
    {
        this.destination = destination;
        this.adminThread = adminCursor;
        this.bufferNotifier = bufferNotifier;
        this.channelId = channelId;
        this.sessionId = sessionId;
        this.channels = channels;
        currentTermId = UNKNOWN_TERM_ID;
        // TODO: notify the channel when you get a term id from the media driver.
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
        // TODO
        return false;
    }

    public boolean offer(final ByteBuffer buffer, final int offset)
    {
        // TODO
        return false;
    }

    public void send(final ByteBuffer buffer) throws BufferExhaustedException
    {
        // TODO
    }

    public void send(final ByteBuffer buffer, final int offset) throws BufferExhaustedException
    {
        // TODO
    }

    public void blockingSend(final ByteBuffer buffer)
    {
        // TODO: Is this necessary?
    }

    public void blockingSend(final ByteBuffer buffer, final int offset)
    {
        // TODO: Is this necessary?
    }

    private void requestTerm(final long termId)
    {
        adminThread.sendRequestTerm(channelId, termId, destination);
    }

    private void startTerm()
    {
        bufferNotifier.termBuffer(currentTermId);
    }

    private void rollTerm()
    {
        bufferNotifier.endOfTermBuffer(currentTermId);
        currentTermId++;
        requestTerm(currentTermId + 1);
        startTerm();
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

    public TermBufferNotifier bufferNotifier()
    {
        return bufferNotifier;
    }

}
