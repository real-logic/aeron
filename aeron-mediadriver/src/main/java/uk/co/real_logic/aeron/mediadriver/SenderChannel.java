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
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogScanner;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Encapsulates the information associated with a channel
 * to send on. Processed in the SenderThread.
 */
public class SenderChannel
{
    /** initial heartbeat timeout (cancelled by SM) */
    public static final int INITIAL_HEARTBEAT_TIMEOUT_MS = 100;
    public static final long INITIAL_HEARTBEAT_TIMEOUT_NS = MILLISECONDS.toNanos(INITIAL_HEARTBEAT_TIMEOUT_MS);
    /** heartbeat after data sent */
    public static final int HEARTBEAT_TIMEOUT_MS = 500;
    public static final long HEARTBEAT_TIMEOUT_NS = MILLISECONDS.toNanos(HEARTBEAT_TIMEOUT_MS);

    private final ControlFrameHandler frameHandler;
    private final SenderControlStrategy controlStrategy;

    private final BufferRotator buffers;
    private final UdpDestination destination;
    private final long sessionId;
    private final long channelId;

    private final AtomicLong currentTermId;
    private final AtomicLong cleanedTermId;

    // TODO: temporary. Replace with counter.
    private final AtomicLong timeOfLastSendOrHeartbeat;

    private final int headerLength;
    private final int mtuLength;
    private final ByteBuffer scratchSendBuffer = ByteBuffer.allocateDirect(DataHeaderFlyweight.HEADER_LENGTH);
    private final AtomicBuffer scratchAtomicBuffer = new AtomicBuffer(scratchSendBuffer);
    private final LogScanner[] scanners;

    private final ByteBuffer[] termSendBuffers;
    private final SenderFlowControlState activeFlowControlState = new SenderFlowControlState(0);
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private int currentIndex = 0;
    private int statusMessagesSeen = 0;

    public SenderChannel(final ControlFrameHandler frameHandler,
                         final SenderControlStrategy controlStrategy,
                         final BufferRotator buffers,
                         final UdpDestination destination,
                         final long sessionId,
                         final long channelId,
                         final long initialTermId,
                         final int headerLength,
                         final int mtuLength)
    {
        this.frameHandler = frameHandler;
        this.controlStrategy = controlStrategy;
        this.buffers = buffers;
        this.destination = destination;
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.headerLength = headerLength;
        this.mtuLength = mtuLength;

        scanners = buffers.buffers().map(this::newScanner).toArray(LogScanner[]::new);
        termSendBuffers = buffers.buffers().map(this::duplicateLogBuffer).toArray(ByteBuffer[]::new);

        currentTermId = new AtomicLong(initialTermId);
        cleanedTermId = new AtomicLong(initialTermId + 2);
        timeOfLastSendOrHeartbeat = new AtomicLong(frameHandler.currentTime());
    }

    private ByteBuffer duplicateLogBuffer(final LogBuffers log)
    {
        final ByteBuffer buffer = log.logBuffer().duplicateByteBuffer();
        buffer.clear();

        return buffer;
    }

    public LogScanner newScanner(final LogBuffers log)
    {
        return new LogScanner(log.logBuffer(), log.stateBuffer(), headerLength);
    }

    public void send()
    {
        // TODO: blocking due to flow control
        // read from term buffer
        try
        {
            final int frameSequenceNumber = 0;  // TODO: grab this from peeking at the frame
            final int rightEdge = activeFlowControlState.rightEdgeOfWindowVolatile();
            final int availableBuffer = rightEdge - frameSequenceNumber;
            final int maxLength = Math.min(availableBuffer, mtuLength);

            final LogScanner.AvailabilityHandler handler = (offset, length) ->
            {
                // at this point sendBuffer wraps the same underlying
                // ByteBuffer as the buffer parameter
                final ByteBuffer sendBuffer = termSendBuffers[currentIndex];

                sendBuffer.position(offset);
                sendBuffer.limit(offset + length);

                try
                {
                    final int bytesSent = frameHandler.send(sendBuffer);
                    if (length != bytesSent)
                    {
                        // TODO: error
                    }

                    timeOfLastSendOrHeartbeat.lazySet(frameHandler.currentTime());
                }
                catch (final Exception ex)
                {
                    //TODO: errors
                    ex.printStackTrace();
                }
            };

            final LogScanner scanner = scanners[currentIndex];
            scanner.scanNext(maxLength, handler);

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

    /**
     * This is performed on the Media Conductor thread
     */
    public void onStatusMessage(final long termId,
                                final long highestContiguousSequenceNumber,
                                final long receiverWindow)
    {
        final int rightEdge = controlStrategy.onStatusMessage(termId,
                                                              highestContiguousSequenceNumber,
                                                              receiverWindow);
        activeFlowControlState.rightEdgeOfWindowOrdered(rightEdge);
        statusMessagesSeen++;
    }

    public void sendHeartbeat()
    {
        // called from conductor thread on timeout

        // used for both initial setup 0 length data as well as heartbeats

        // send 0 length data frame with current termOffset
        dataHeader.wrap(scratchAtomicBuffer, 0);

        dataHeader.sessionId(sessionId)
                  .channelId(channelId)
                  .termId(currentTermId.get())
                  .termOffset(0)
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
                // TODO: log or count error
                System.out.println("Error sending heartbeat packet");
            }

            timeOfLastSendOrHeartbeat.lazySet(frameHandler.currentTime());
        }
        catch (final Exception ex)
        {
            //TODO: errors
            ex.printStackTrace();
        }
    }

    public void heartbeatCheck()
    {
        // TODO: now should be cached in TimerWheel to avoid too many calls
        if (statusMessagesSeen > 0)
        {
            if ((frameHandler.currentTime() - timeOfLastSendOrHeartbeat.get()) > HEARTBEAT_TIMEOUT_NS)
            {
                sendHeartbeat();
            }
        }
        else
        {
            if ((frameHandler.currentTime() - timeOfLastSendOrHeartbeat.get()) > INITIAL_HEARTBEAT_TIMEOUT_NS)
            {
                sendHeartbeat();
            }
        }
    }

    /**
     * This is performed on the Media Conductor thread
     */
    public void processBufferRotation()
    {
        final long requiredCleanTermId = currentTermId.get() + 1;
        if (requiredCleanTermId > cleanedTermId.get())
        {
            try
            {
                buffers.rotate();
                cleanedTermId.lazySet(requiredCleanTermId);
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
