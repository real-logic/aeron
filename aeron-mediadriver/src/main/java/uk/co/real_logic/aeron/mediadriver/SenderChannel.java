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
 *
 * Stores Flow Control State
 */
public class SenderChannel
{

    private final SrcFrameHandler frameHandler;
    private final long sessionId;
    private final long channelId;
    private final long termId;
    private final ByteBuffer producerBuffer;
    private final RingBuffer ringBuffer;
    private final ByteBuffer sendBuffer;
    private final SenderFlowControlState activeFlowControlState;

    public SenderChannel(final SrcFrameHandler frameHandler,
                         final ByteBuffer producerBuffer,
                         final long sessionId,
                         final long channelId,
                         final long termId)
    {
        this.frameHandler = frameHandler;
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.termId = termId;
        this.producerBuffer = producerBuffer;
        this.ringBuffer = new ManyToOneRingBuffer(new AtomicBuffer(producerBuffer));
        this.activeFlowControlState = new SenderFlowControlState(0, 0);
        this.sendBuffer = ByteBuffer.allocateDirect(MediaDriver.READ_BYTE_BUFFER_SZ);  // TODO: need to make this cfg
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
                    buffer.getBytes(index, sendBuffer, length); // fill in the buffer to send by copying
                }, 1);

                // send the frame we just copied out.... yes, it would be great to not copy...
                int bytesSent = frameHandler.send(sendBuffer);
            }
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

    public long sessionId()
    {
        return sessionId;
    }

    public long channelId()
    {
        return channelId;
    }

    // only called by admin thread on SM reception
    public void updateFlowControlState(final int highestContiguousSequenceNumber, final int receiverWindow)
    {
    }
}
