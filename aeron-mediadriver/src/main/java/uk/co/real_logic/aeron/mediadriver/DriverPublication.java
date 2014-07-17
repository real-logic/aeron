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

import uk.co.real_logic.aeron.mediadriver.buffer.TermBuffers;
import uk.co.real_logic.aeron.mediadriver.buffer.RawLog;
import uk.co.real_logic.aeron.util.TermHelper;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.*;
import uk.co.real_logic.aeron.util.event.EventCode;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.status.BufferPositionReporter;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.TermHelper.termIdToBufferIndex;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.IN_CLEANING;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.NEEDS_CLEANING;

/**
 * Publication to be sent to registered subscribers.
 */
public class DriverPublication
{
    private static final EventLogger LOGGER = new EventLogger(DriverPublication.class);

    /** Initial heartbeat timeout (cancelled by SM) */
    public static final int INITIAL_HEARTBEAT_TIMEOUT_MS = 100;
    public static final long INITIAL_HEARTBEAT_TIMEOUT_NS = MILLISECONDS.toNanos(INITIAL_HEARTBEAT_TIMEOUT_MS);
    /** Heartbeat after data sent */
    public static final int HEARTBEAT_TIMEOUT_MS = 500;
    public static final long HEARTBEAT_TIMEOUT_NS = MILLISECONDS.toNanos(HEARTBEAT_TIMEOUT_MS);

    private final TimerWheel timerWheel;

    private final long sessionId;
    private final long channelId;

    private final AtomicLong activeTermId;

    private final AtomicLong timeOfLastSendOrHeartbeat;

    private final int headerLength;
    private final int mtuLength;
    private final ByteBuffer scratchByteBuffer = ByteBuffer.allocateDirect(DataHeaderFlyweight.HEADER_LENGTH);
    private final AtomicBuffer scratchAtomicBuffer = new AtomicBuffer(scratchByteBuffer);
    private final LogScanner[] scanners;
    private final RetransmitHandler[] retransmitHandlers;

    private final ByteBuffer[] termSendBuffers;
    private final ByteBuffer[] termRetransmitBuffers;

    private final SenderControlStrategy controlStrategy;
    private final AtomicLong rightEdge;
    private final ControlFrameHandler frameHandler;
    private final BufferPositionReporter limitReporter;

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final DataHeaderFlyweight retransmitDataHeader = new DataHeaderFlyweight();

    private final int positionBitsToShift;
    private final long initialPosition;

    private int nextTermOffset = 0;
    private int activeIndex = 0;
    private int statusMessagesSeen = 0;
    private int shiftsForTermId;

    private final InetSocketAddress dstAddress;

    public DriverPublication(final ControlFrameHandler frameHandler,
                             final TimerWheel timerWheel,
                             final SenderControlStrategy controlStrategy,
                             final TermBuffers termBuffers,
                             final BufferPositionReporter limitReporter,
                             final long sessionId,
                             final long channelId,
                             final long initialTermId,
                             final int headerLength,
                             final int mtuLength)
    {
        this.frameHandler = frameHandler;
        this.dstAddress = frameHandler.destination().remoteData();
        this.controlStrategy = controlStrategy;
        this.timerWheel = timerWheel;
        this.limitReporter = limitReporter;
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.headerLength = headerLength;
        this.mtuLength = mtuLength;
        this.activeIndex = termIdToBufferIndex(initialTermId);

        scanners = termBuffers.stream().map(this::newScanner).toArray(LogScanner[]::new);
        termSendBuffers = termBuffers.stream().map(this::duplicateLogBuffer).toArray(ByteBuffer[]::new);
        termRetransmitBuffers = termBuffers.stream().map(this::duplicateLogBuffer).toArray(ByteBuffer[]::new);
        retransmitHandlers = termBuffers.stream().map(this::newRetransmitHandler).toArray(RetransmitHandler[]::new);

        final int termCapacity = scanners[0].capacity();
        rightEdge = new AtomicLong(controlStrategy.initialRightEdge(initialTermId, termCapacity));
        shiftsForTermId = Long.numberOfTrailingZeros(termCapacity);

        activeTermId = new AtomicLong(initialTermId);
        timeOfLastSendOrHeartbeat = new AtomicLong(this.timerWheel.now());

        this.positionBitsToShift = Integer.numberOfTrailingZeros(termCapacity);
        this.initialPosition = initialTermId << positionBitsToShift;
        limitReporter.position(termCapacity);
    }

    public int send()
    {
        int workCount = 0;
        try
        {
            final long nextOffset = (activeTermId.get() << shiftsForTermId) + nextTermOffset;
            final int availableWindow = (int)(rightEdge.get() - nextOffset);
            final int scanLimit = Math.min(availableWindow, mtuLength);

            final LogScanner scanner = scanners[activeIndex];
            workCount += scanner.scanNext(this::onSendFrame, scanLimit);

            if (scanner.isComplete())
            {
                activeIndex = TermHelper.rotateNext(activeIndex);
                activeTermId.lazySet(activeTermId.get() + 1);
            }

            limitReporter.position(position(scanner.tail()) + scanner.capacity());
        }
        catch (final Exception ex)
        {
            LOGGER.logException(ex);
        }

        return workCount;
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
                                final long receiverWindow,
                                final InetSocketAddress address)
    {
        final long newRightEdge =
            controlStrategy.onStatusMessage(termId, highestContiguousSequenceNumber, receiverWindow, address);

        rightEdge.lazySet(newRightEdge);

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
    public int cleanLogBuffer()
    {
        for (final LogBuffer logBuffer : scanners)
        {
            if (logBuffer.status() == NEEDS_CLEANING && logBuffer.compareAndSetStatus(NEEDS_CLEANING, IN_CLEANING))
            {
                logBuffer.clean();

                return 1;
            }
        }

        return 0;
    }

    private ByteBuffer duplicateLogBuffer(final RawLog log)
    {
        final ByteBuffer buffer = log.logBuffer().duplicateByteBuffer();
        buffer.clear();

        return buffer;
    }

    private LogScanner newScanner(final RawLog log)
    {
        return new LogScanner(log.logBuffer(), log.stateBuffer(), headerLength);
    }

    private RetransmitHandler newRetransmitHandler(final RawLog log)
    {
        return new RetransmitHandler(new LogReader(log.logBuffer(), log.stateBuffer()),
                                     timerWheel,
                                     MediaConductor.RETRANS_UNICAST_DELAY_GENERATOR,
                                     MediaConductor.RETRANS_UNICAST_LINGER_GENERATOR,
                                     this::onSendRetransmit);
    }

    private int determineIndexByTermId(final long termId)
    {
        if (termId == activeTermId.get())
        {
            return activeIndex;
        }

        // TODO: this needs to account for rotation
        return -1;
    }

    /*
     *
     * Function used as a lambda for LogScanner.AvailabilityHandler
     */
    private void onSendFrame(final int offset, final int length)
    {
        // at this point sendBuffer wraps the same underlying
        // ByteBuffer as the buffer parameter
        final ByteBuffer sendBuffer = termSendBuffers[activeIndex];

        // could wrap and use DataHeader to grab specific fields, e.g. dataHeader.wrap(sendBuffer, offset);
        sendBuffer.limit(offset + length);
        sendBuffer.position(offset);

        try
        {
            final int bytesSent = frameHandler.sendTo(sendBuffer, dstAddress);
            if (length != bytesSent)
            {
                throw new IllegalStateException("could not send all of message: " + bytesSent + "/" + length);
            }

            timeOfLastSendOrHeartbeat.lazySet(timerWheel.now());

            nextTermOffset = align(offset + length, FrameDescriptor.FRAME_ALIGNMENT);
        }
        catch (final Exception ex)
        {
            LOGGER.logException(ex);
        }
    }

    /**
     * This is performed on the Media Conductor thread via the RetransmitHandler
     */
    private void onSendRetransmit(final AtomicBuffer buffer, final int offset, final int length)
    {
        // use termRetransmitBuffers, but need to know which one... so, use DataHeaderFlyweight to grab it
        retransmitDataHeader.wrap(buffer, offset);
        final int index = determineIndexByTermId(retransmitDataHeader.termId());

        if (-1 != index)
        {
            final ByteBuffer termRetransmitBuffer = termRetransmitBuffers[index];
            termRetransmitBuffer.position(offset);
            termRetransmitBuffer.limit(offset + length);

            try
            {
                final int bytesSent = frameHandler.sendTo(termRetransmitBuffer, dstAddress);
                if (bytesSent != length)
                {
                    LOGGER.log(EventCode.COULD_NOT_FIND_INTERFACE, termRetransmitBuffer, length, dstAddress);
                }
            }
            catch (final Exception ex)
            {
                LOGGER.logException(ex);
            }
        }
    }

    private void sendHeartbeat()
    {
        // called from conductor thread on timeout

        // used for both initial setup 0 length data as well as heartbeats

        // send 0 length data frame with current termOffset
        dataHeader.wrap(scratchAtomicBuffer, 0);

        dataHeader.sessionId(sessionId)
                  .channelId(channelId)
                  .termId(activeTermId.get())
                  .termOffset(nextTermOffset)
                  .frameLength(DataHeaderFlyweight.HEADER_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        scratchByteBuffer.position(0);
        scratchByteBuffer.limit(DataHeaderFlyweight.HEADER_LENGTH);

        try
        {
            final int bytesSent = frameHandler.sendTo(scratchByteBuffer, dstAddress);
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                LOGGER.log(EventCode.ERROR_SENDING_HEARTBEAT_PACKET, scratchByteBuffer,
                           DataHeaderFlyweight.HEADER_LENGTH, dstAddress);
            }

            timeOfLastSendOrHeartbeat.lazySet(timerWheel.now());
        }
        catch (final Exception ex)
        {
            LOGGER.logException(ex);
        }
    }

    private long position(final int currentTail)
    {
        return TermHelper.calculatePosition(currentTail, activeTermId.get(), positionBitsToShift, initialPosition);
    }
}
