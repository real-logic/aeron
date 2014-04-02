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

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

/**
 * Encapsulates the information associated with a channel
 * to send on. Processed in the SenderThread.
 */
public class SenderChannel
{
    public static final int STATE_PENDING = 0;
    public static final int STATE_READY_FOR_SM = 1;

    private final SrcFrameHandler frameHandler;
    private final UdpDestination destination;
    private final long sessionId;
    private final long channelId;
    private final long termId;
    private final ByteBuffer producerBuffer;
    private final RingBuffer ringBuffer;
    private final ByteBuffer sendBuffer;
    private final SenderFlowControlState activeFlowControlState;
    private int state;

    public SenderChannel(final SrcFrameHandler frameHandler,
                         final ByteBuffer producerBuffer,
                         final UdpDestination destination,
                         final long sessionId,
                         final long channelId,
                         final long termId)
    {
        this.frameHandler = frameHandler;
        this.destination = destination;
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.termId = termId;
        this.producerBuffer = producerBuffer;
        this.ringBuffer = new ManyToOneRingBuffer(new AtomicBuffer(producerBuffer));
        this.activeFlowControlState = new SenderFlowControlState(0, 0);
        this.sendBuffer = producerBuffer.duplicate();
        this.sendBuffer.clear();
        this.state = STATE_PENDING;
    }

    public void process()
    {
        // TODO: blocking due to flow control
        // read from term buffer
        try
        {
            while (true)
            {
                final int frameSequenceNumber = 0;  // TODO: grab this from peeking at the frame
                final int frameLength = 1000;       // TODO: grab this from peeking at the frame
                final int rightEdge = activeFlowControlState.rightEdgeOfWindow();

                // if we can't send, then break out of the loop
                if (frameSequenceNumber + frameLength > rightEdge)
                {
                    break;
                }

                // frame will fit in the window, read and send just 1
                ringBuffer.read((eventTypeId, buffer, index, length) ->
                {
                    // at this point sendBuffer wraps the same underlying
                    // bytebuffer as the buffer parameter
                    sendBuffer.position(index);
                    sendBuffer.limit(index + length);

                    try
                    {
                        int bytesSent = frameHandler.send(sendBuffer);
                    }
                    catch (Exception e)
                    {
                        //TODO: errors
                        e.printStackTrace();
                    }

                }, 1);
            }
        }
        catch (final Exception e)
        {
            // TODO: error logging
            e.printStackTrace();
        }
    }

    public int state()
    {
        return state;
    }

    public void state(final int state)
    {
        this.state = state;
    }

    public boolean isOpen()
    {
        return frameHandler.isOpen();
    }

    public UdpDestination destination()
    {
        return destination;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public long channelId()
    {
        return channelId;
    }

    public long termId()
    {
        return termId;
    }

    public SenderFlowControlState flowControlState()
    {
        return activeFlowControlState;
    }
}
