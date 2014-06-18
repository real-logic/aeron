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
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogScanner;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.aeron.util.BitUtil.align;

/**
 * Encapsulates the information associated with a channel
 * to send on. Processed in the SenderThread.
 */
public class SenderChannel
{
    /**
     * Interface for changing or redirecting sending (mostly testing)
     */
    public interface SendFunction
    {
        int sendTo(final ByteBuffer buffer, final InetSocketAddress address) throws Exception;
    }

    /** initial heartbeat timeout (cancelled by SM) */
    public static final int INITIAL_HEARTBEAT_TIMEOUT_MS = 100;
    public static final long INITIAL_HEARTBEAT_TIMEOUT_NS = MILLISECONDS.toNanos(INITIAL_HEARTBEAT_TIMEOUT_MS);
    /** heartbeat after data sent */
    public static final int HEARTBEAT_TIMEOUT_MS = 500;
    public static final long HEARTBEAT_TIMEOUT_NS = MILLISECONDS.toNanos(HEARTBEAT_TIMEOUT_MS);

    private final SenderControlStrategy controlStrategy;
    private final TimerWheel timerWheel;

    private final BufferRotator buffers;
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
    private final RetransmitHandler retransmitHandlers[];

    private final ByteBuffer[] termSendBuffers;
    private final ByteBuffer[] termRetransmitBuffers;

    private final SenderFlowControlState activeFlowControlState = new SenderFlowControlState(0);

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final DataHeaderFlyweight retransmitDataHeader = new DataHeaderFlyweight();

    private int currentIndex = 0;
    private int statusMessagesSeen = 0;
    private long nextOffset = 0;

    private final SendFunction sendFunction;
    private final InetSocketAddress dstAddress;

    public SenderChannel(final ControlFrameHandler frameHandler,
                         final TimerWheel timerWheel,
                         final SenderControlStrategy controlStrategy,
                         final BufferRotator buffers,
                         final long sessionId,
                         final long channelId,
                         final long initialTermId,
                         final int headerLength,
                         final int mtuLength,
                         final SendFunction sendFunction)
    {
        this.dstAddress = frameHandler.destination().remoteData();
        this.controlStrategy = controlStrategy;
        this.timerWheel = timerWheel;
        this.buffers = buffers;
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.headerLength = headerLength;
        this.mtuLength = mtuLength;
        this.sendFunction = sendFunction;

        scanners = buffers.buffers().map(this::newScanner).toArray(LogScanner[]::new);
        termSendBuffers = buffers.buffers().map(this::duplicateLogBuffer).toArray(ByteBuffer[]::new);
        termRetransmitBuffers = buffers.buffers().map(this::duplicateLogBuffer).toArray(ByteBuffer[]::new);
        retransmitHandlers = buffers.buffers().map(this::newRetransmitHandler).toArray(RetransmitHandler[]::new);

        currentTermId = new AtomicLong(initialTermId);
        cleanedTermId = new AtomicLong(initialTermId + 2);
        timeOfLastSendOrHeartbeat = new AtomicLong(this.timerWheel.now());
    }

    public boolean send()
    {
        boolean hasDoneWork = false;
        try
        {
            final int rightEdge = activeFlowControlState.rightEdgeOfWindowVolatile();
            final int availableBuffer = rightEdge - (int)nextOffset;
            final int maxLength = Math.min(availableBuffer, mtuLength);

            final LogScanner.AvailabilityHandler handler = (offset, length) ->
            {
                // at this point sendBuffer wraps the same underlying
                // ByteBuffer as the buffer parameter
                final ByteBuffer sendBuffer = termSendBuffers[currentIndex];

                dataHeader.wrap(sendBuffer, offset);
                sendBuffer.limit(offset + length);
                sendBuffer.position(offset);

                try
                {
                    final int bytesSent = sendFunction.sendTo(sendBuffer, dstAddress);
                    if (length != bytesSent)
                    {
                        throw new IllegalStateException("could not send all of message: " + bytesSent + "/" +
                                                        dataHeader.frameLength());
                        // TODO: error
                    }

                    timeOfLastSendOrHeartbeat.lazySet(timerWheel.now());

                    nextOffset = align(offset + length, FrameDescriptor.FRAME_ALIGNMENT);
                }
                catch (final Exception ex)
                {
                    //TODO: errors
                    ex.printStackTrace();
                }
            };

            final LogScanner scanner = scanners[currentIndex];
            hasDoneWork = scanner.scanNext(maxLength, handler);

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

        return hasDoneWork;
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

    /**
     * This is performed on the Media Conductor thread
     */
    public void onNakFrame(final long termId, final long termOffset, final long length)
    {
        final int index = determineIndexByTermId(termId);

        if (-1 != index)
        {
            retransmitHandlers[index].onNak((int)termOffset);
        }
    }

    /**
     * This is performed on the Media Conductor thread
     */
    private void onSendRetransmit(final AtomicBuffer buffer, final int offset, final int length)
    {
        // use termRetransmitBuffers, but need to know which one... so, use DataHeaderFlyweight to grab it
        retransmitDataHeader.wrap(buffer, offset);
        final int index = determineIndexByTermId(retransmitDataHeader.termId());

        if (-1 != index)
        {
            termRetransmitBuffers[index].position(offset);
            termRetransmitBuffers[index].limit(offset + length);

            try
            {
                final int bytesSent = sendFunction.sendTo(termRetransmitBuffers[index], dstAddress);
                if (bytesSent != length)
                {
                    System.err.println("could not send entire retransmit");
                }
            }
            catch (final Exception ex)
            {
                ex.printStackTrace();
            }
        }
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
                  .termOffset(nextOffset)
                  .frameLength(DataHeaderFlyweight.HEADER_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        scratchSendBuffer.position(0);
        scratchSendBuffer.limit(DataHeaderFlyweight.HEADER_LENGTH);

        try
        {
            final int bytesSent = sendFunction.sendTo(scratchSendBuffer, dstAddress);
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                // TODO: log or count error
                System.out.println("Error sending heartbeat packet");
            }

            timeOfLastSendOrHeartbeat.lazySet(timerWheel.now());
        }
        catch (final Exception ex)
        {
            //TODO: errors
            ex.printStackTrace();
        }
    }

    public boolean heartbeatCheck()
    {
        boolean heartbeatSent = false;

        if (statusMessagesSeen > 0)
        {
            if ((timerWheel.now() - timeOfLastSendOrHeartbeat.get()) > HEARTBEAT_TIMEOUT_NS)
            {
                sendHeartbeat();
                heartbeatSent = true;
            }
        }
        else
        {
            if ((timerWheel.now() - timeOfLastSendOrHeartbeat.get()) > INITIAL_HEARTBEAT_TIMEOUT_NS)
            {
                sendHeartbeat();
                heartbeatSent = true;
            }
        }

        return heartbeatSent;
    }

    /**
     * This is performed on the Media Conductor thread
     */
    public boolean processBufferRotation()
    {
        boolean rotated = false;
        final long requiredCleanTermId = currentTermId.get() + 1;
        if (requiredCleanTermId > cleanedTermId.get())
        {
            try
            {
                buffers.rotate();
                cleanedTermId.lazySet(requiredCleanTermId);
                rotated = true;
            }
            catch (final IOException ex)
            {
                // TODO: log exception
                // TODO: probably should deal with stopping this all together
                ex.printStackTrace();
            }
        }

        return rotated;
    }

    private ByteBuffer duplicateLogBuffer(final LogBuffers log)
    {
        final ByteBuffer buffer = log.logBuffer().duplicateByteBuffer();
        buffer.clear();

        return buffer;
    }

    private LogScanner newScanner(final LogBuffers log)
    {
        return new LogScanner(log.logBuffer(), log.stateBuffer(), headerLength);
    }

    private RetransmitHandler newRetransmitHandler(final LogBuffers log)
    {
        return new RetransmitHandler(new LogReader(log.logBuffer(), log.stateBuffer()),
            timerWheel, MediaConductor.RETRANS_UNICAST_DELAY_GENERATOR,
            MediaConductor.RETRANS_UNICAST_LINGER_GENERATOR, this::onSendRetransmit);
    }

    private int determineIndexByTermId(final long termId)
    {
        if (termId == currentTermId.get())
        {
            return currentIndex;
        }

        // this needs to account for rotation
        return -1;
    }
}
