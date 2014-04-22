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

import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.mediadriver.buffer.LogBuffers;
import uk.co.real_logic.aeron.mediadriver.buffer.MappedBufferRotator;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.MtuScanner;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.util.BitUtil.next;

/**
 * Encapsulates the information associated with a channel
 * to send on. Processed in the SenderThread.
 */
public class SenderChannel
{
    public static final int BUFFER_COUNT = 3;
    private final ControlFrameHandler frameHandler;
    private final SenderFlowControlStrategy flowControlStrategy;
    private final UdpDestination destination;
    private final long sessionId;
    private final long channelId;
    private final long currentTermId;

    private int currentIndex;

    private final MtuScanner[] scanners;

    /** duplicate log buffers to work around the lack of a (buffer, start, length) send method */
    private final ByteBuffer[] sendBuffers;

    private final SenderFlowControlState activeFlowControlState;

    public SenderChannel(final ControlFrameHandler frameHandler,
                         final SenderFlowControlStrategy flowControlStrategy,
                         final BufferRotator buffers,
                         final UdpDestination destination,
                         final long sessionId,
                         final long channelId,
                         final long initialTermId)
    {
        this.frameHandler = frameHandler;
        this.flowControlStrategy = flowControlStrategy;
        this.destination = destination;
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.currentTermId = initialTermId;

        scanners =  buffers.buffers()
                           .map(this::newScanner)
                           .toArray(MtuScanner[]::new);

        activeFlowControlState = new SenderFlowControlState(0);

        sendBuffers = buffers.buffers()
                             .map(this::duplicateLogBuffer)
                             .toArray(ByteBuffer[]::new);
        currentIndex = 0;
    }

    private ByteBuffer duplicateLogBuffer(final LogBuffers log)
    {
        final ByteBuffer buffer = log.logBuffer().duplicateByteBuffer();
        buffer.clear();
        return buffer;
    }

    public MtuScanner newScanner(final LogBuffers log)
    {
        // TODO
        final int headerLength = 16;
        final int mtuLength = 640;

        return new MtuScanner(log.logBuffer(), log.stateBuffer(), mtuLength, headerLength);
    }

    public void process()
    {
        // TODO: blocking due to flow control
        // read from term buffer
        try
        {
            final int frameSequenceNumber = 0;  // TODO: grab this from peeking at the frame
            final int frameLength = 1000;       // TODO: grab this from peeking at the frame
            final int rightEdge = activeFlowControlState.rightEdgeOfWindowAtomic();

            // if we can't send, then break out of the loop
            if (frameSequenceNumber + frameLength > rightEdge)
            {
                return;
            }

            final MtuScanner scanner = scanners[currentIndex];
            if (scanner.scanNext())
            {
                // at this point sendBuffer wraps the same underlying
                // bytebuffer as the buffer parameter
                final ByteBuffer sendBuffer = sendBuffers[currentIndex];

                final int offset = scanner.offset();
                final int expectedBytesSent = scanner.length();
                sendBuffer.position(offset);
                sendBuffer.limit(offset + expectedBytesSent);

                try
                {
                    int bytesSent = frameHandler.send(sendBuffer);
                    if (expectedBytesSent != bytesSent)
                    {
                        // TODO: error
                    }
                }
                catch (Exception e)
                {
                    //TODO: errors
                    e.printStackTrace();
                }
            }

            if (scanner.isComplete())
            {
                currentIndex = next(currentIndex, BUFFER_COUNT);
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

    public long currentTermId()
    {
        return currentTermId;
    }

    public void removeTerm(final long termId)
    {
        // TODO: get rid of reference to old term Id. If this is current termId, then stop, etc.
    }

    public void onStatusMessage(final long termId,
                                final long highestContiguousSequenceNumber,
                                final long receiverWindow)
    {
        final int rightEdge = flowControlStrategy.onStatusMessage(termId,
                                                                  highestContiguousSequenceNumber,
                                                                  receiverWindow);
        activeFlowControlState.updateRightEdgeOfWindow(rightEdge);
    }

}
