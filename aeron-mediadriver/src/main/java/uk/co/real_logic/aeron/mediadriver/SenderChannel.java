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
import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.MtuScanner;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.CLEAN_WINDOW;

/**
 * Encapsulates the information associated with a channel
 * to send on. Processed in the SenderThread.
 */
public class SenderChannel
{
    public static final int FLOW_CONTROL_TIMEOUT_MILLISECONDS = 100;

    private final ControlFrameHandler frameHandler;
    private final SenderFlowControlStrategy flowControlStrategy;

    private final BufferRotator buffers;
    private final UdpDestination destination;
    private final long sessionId;
    private final long channelId;

    private final AtomicLong currentTermId;
    private final AtomicLong cleanedTermId;

    private final int headerLength;
    private final int mtuLength;
    private TimerWheel.Timer flowControlTimer;
    private final ByteBuffer scratchSendBuffer = ByteBuffer.allocateDirect(DataHeaderFlyweight.HEADER_LENGTH);
    private final AtomicBuffer scratchAtomicBuffer = new AtomicBuffer(scratchSendBuffer);

    private int currentIndex;

    private final MtuScanner[] scanners;

    /** duplicate log buffers to work around the lack of a (buffer, start, length) send method */
    private final ByteBuffer[] sendBuffers;

    private final SenderFlowControlState activeFlowControlState = new SenderFlowControlState(0);

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    public SenderChannel(final ControlFrameHandler frameHandler,
                         final SenderFlowControlStrategy flowControlStrategy,
                         final BufferRotator buffers,
                         final UdpDestination destination,
                         final long sessionId,
                         final long channelId,
                         final long initialTermId,
                         final int headerLength,
                         final int mtuLength)
    {
        this.frameHandler = frameHandler;
        this.flowControlStrategy = flowControlStrategy;
        this.buffers = buffers;
        this.destination = destination;
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.headerLength = headerLength;
        this.mtuLength = mtuLength;

        scanners = buffers.buffers()
                          .map(this::newScanner)
                          .toArray(MtuScanner[]::new);

        sendBuffers = buffers.buffers()
                             .map(this::duplicateLogBuffer)
                             .toArray(ByteBuffer[]::new);
        currentIndex = 0;
        currentTermId = new AtomicLong(initialTermId);
        cleanedTermId = new AtomicLong(initialTermId + 2);

        this.flowControlTimer = frameHandler.newTimeout(FLOW_CONTROL_TIMEOUT_MILLISECONDS,
                                                        TimeUnit.MILLISECONDS,
                                                        this::onFlowControlTimer);
    }

    private ByteBuffer duplicateLogBuffer(final LogBuffers log)
    {
        final ByteBuffer buffer = log.logBuffer().duplicateByteBuffer();
        buffer.clear();

        return buffer;
    }

    public MtuScanner newScanner(final LogBuffers log)
    {
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
            if ((frameSequenceNumber + frameLength) > rightEdge)
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
                currentIndex = BufferRotationDescriptor.rotateId(currentIndex);
                currentTermId.incrementAndGet();
            }
        }
        catch (final Exception ex)
        {
            // TODO: error logging
            ex.printStackTrace();
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

    public void onStatusMessage(final long termId,
                                final long highestContiguousSequenceNumber,
                                final long receiverWindow)
    {
        final int rightEdge = flowControlStrategy.onStatusMessage(termId,
                                                                  highestContiguousSequenceNumber,
                                                                  receiverWindow);
        activeFlowControlState.updateRightEdgeOfWindow(rightEdge);

        if (flowControlTimer.isActive())
        {
            flowControlTimer.cancel();
        }
    }

    public void onFlowControlTimer()
    {
        // called from conductor thread on timeout

        // used for both initial setup 0 length data as well as heartbeats

        // send 0 length data frame with current sequenceNumber
        dataHeader.wrap(scratchAtomicBuffer, 0);

        dataHeader.sessionId(sessionId)
                  .channelId(channelId)
                  .termId(currentTermId.get())
                  .sequenceNumber(0)
                  .frameLength(DataHeaderFlyweight.HEADER_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        scratchSendBuffer.position(0);
        scratchSendBuffer.limit(DataHeaderFlyweight.HEADER_LENGTH);

        try
        {
            final int bytesSent = frameHandler.send(scratchSendBuffer);
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                // TODO: error
            }
        }
        catch (final Exception ex)
        {
            //TODO: errors
            ex.printStackTrace();
        }
    }

    public void processBufferRotation()
    {
        final long requiredCleanTermid = currentTermId.get() + CLEAN_WINDOW;
        if (requiredCleanTermid > cleanedTermId.get())
        {
            try
            {
                buffers.rotate();
                cleanedTermId.incrementAndGet();
            }
            catch (final IOException ex)
            {
                // TODO: log exception
                // TODO: probably should deal with stopping this all together
                ex.printStackTrace();
            }
        }
    }
}
