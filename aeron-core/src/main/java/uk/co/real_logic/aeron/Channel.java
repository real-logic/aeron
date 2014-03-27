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

import uk.co.real_logic.aeron.admin.ClientAdminCursor;
import uk.co.real_logic.aeron.admin.TermBufferNotifier;

import java.nio.ByteBuffer;

/**
 * Aeron Channel
 */
public class Channel implements AutoCloseable
{
    private final String destination;
    private final ClientAdminCursor adminThread;
    private final TermBufferNotifier bufferNotifier;
    private final long channelId;

    private long currentTermId;

    public Channel(final String destination,
                   final ClientAdminCursor adminCursor,
                   final TermBufferNotifier bufferNotifier,
                   final long channelId)
    {
        this.destination = destination;
        this.adminThread = adminCursor;
        this.bufferNotifier = bufferNotifier;
        this.channelId = channelId;
        currentTermId = -1L;
        requestTerm(0L);
        rollTerm();
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
        adminThread.sendRequestTerm(destination, channelId, termId);
    }

    private void startTerm()
    {
        // TODO: set buffer
        bufferNotifier.termBuffer(currentTermId);
    }

    private void rollTerm()
    {
        bufferNotifier.endOfTermBuffer(currentTermId);
        currentTermId++;
        requestTerm(currentTermId + 1);
        startTerm();
    }

    @Override
    public void close() throws Exception
    {
        adminThread.sendRemoveChannel(destination, channelId);
    }

}
