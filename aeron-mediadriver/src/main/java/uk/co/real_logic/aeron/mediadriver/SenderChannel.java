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
package uk.co.real_logic.aeron.mediadriver;

import java.nio.ByteBuffer;

/**
 * Encapsulates the information associated with a channel
 * to send on. Processed in the SenderThread.
 *
 * Stores Flow Control State
 */
public class SenderChannel implements AutoCloseable
{

    private final UdpDestination destination;
    private final UdpTransport transport;
    private final BufferManagementStrategy bufferManagementStrategy;
    private final long sessionId;
    private final long channelId;

    public SenderChannel(final UdpDestination destination,
                         final UdpTransport transport,
                         final BufferManagementStrategy bufferManagementStrategy,
                         final long sessionId,
                         final long channelId)
    {
        this.destination = destination;
        this.transport = transport;
        this.bufferManagementStrategy = bufferManagementStrategy;
        this.sessionId = sessionId;
        this.channelId = channelId;
    }

    public void process()
    {
        // TODO: blocking due to flow control
        // read from term buffer
        final ByteBuffer buffer = null;
        try
        {
            int bytesSent = transport.sendTo(buffer, destination.remote());
            // TODO: error condition
        }
        catch (final Exception e)
        {
            // TODO: error logging
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception
    {
        // TODO:
    }

    public void initiateTermBuffers()
    {
        final long termId = (long)(Math.random() * 0xFFFFFFFFL);  // FIXME: this may not be random enough

        // create the buffer, but hold onto it in the strategy. The senderThread will do a lookup on it
        try
        {
            bufferManagementStrategy.addSenderTerm(destination, sessionId, channelId, termId);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public long sessionId()
    {
        return sessionId;
    }

    public long channelId()
    {
        return channelId;
    }

}
