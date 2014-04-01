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

import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagementStrategy;

import java.nio.ByteBuffer;

/**
 * Encapsulates the information associated with a channel
 * to send on. Processed in the SenderThread.
 *
 * Stores Flow Control State
 */
public class SenderChannel
{

    private final SrcFrameHandler frameHandler;
    private final BufferManagementStrategy bufferManagementStrategy;
    private final long sessionId;
    private final long channelId;

    public SenderChannel(final SrcFrameHandler frameHandler,
                         final BufferManagementStrategy bufferManagementStrategy,
                         final long sessionId,
                         final long channelId)
    {
        this.frameHandler = frameHandler;
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
            int bytesSent = frameHandler.send(buffer);
            // TODO: error condition
        }
        catch (final Exception e)
        {
            // TODO: error logging
            e.printStackTrace();
        }
    }

    public boolean isOpen()
    {
        return frameHandler.isOpen();
    }

    public void initiateTermBuffers() throws Exception
    {
        final long termId = (long)(Math.random() * 0xFFFFFFFFL);  // FIXME: this may not be random enough

        // create the buffer, but hold onto it in the strategy. The senderThread will do a lookup on it
        bufferManagementStrategy.addSenderTerm(frameHandler.destination(), sessionId, channelId, termId);
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
