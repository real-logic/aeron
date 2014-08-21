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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogScanner;
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.aeron.common.TermHelper.termIdToBufferIndex;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogScanner.AvailabilityHandler;

/**
 * Publication to be sent to registered subscribers.
 */
public class DriverPublication implements AutoCloseable
{
    /** Initial heartbeat timeout (cancelled by SM) */
    public static final int INITIAL_HEARTBEAT_TIMEOUT_MS = 100;
    public static final long INITIAL_HEARTBEAT_TIMEOUT_NS = MILLISECONDS.toNanos(INITIAL_HEARTBEAT_TIMEOUT_MS);

    /** Heartbeat after data sent */
    public static final int HEARTBEAT_TIMEOUT_MS = 200;
    public static final long HEARTBEAT_TIMEOUT_NS = MILLISECONDS.toNanos(HEARTBEAT_TIMEOUT_MS);

    /** Publication is still active. */
    public static final int ACTIVE = 1;

    /** Client is now closed and stream should be flushed and then cleaned up. */
    public static final int EOF = 2;

    /** Publication has been flushed to the media. */
    public static final int FLUSHED = 3;

    private final TimerWheel timerWheel;
    private final int sessionId;
    private final int streamId;
    private final AtomicInteger activeTermId;
    private final int headerLength;
    private final int mtuLength;

    private final ByteBuffer scratchByteBuffer = ByteBuffer.allocateDirect(DataHeaderFlyweight.HEADER_LENGTH);
    private final AtomicBuffer scratchAtomicBuffer = new AtomicBuffer(scratchByteBuffer);
    private final LogScanner[] logScanners = new LogScanner[TermHelper.BUFFER_COUNT];
    private final LogScanner[] retransmitLogScanners = new LogScanner[TermHelper.BUFFER_COUNT];
    private final ByteBuffer[] sendBuffers = new ByteBuffer[TermHelper.BUFFER_COUNT];

    private final AtomicLong positionLimit;
    private final SendChannelEndpoint mediaEndpoint;
    private final TermBuffers termBuffers;
    private final PositionReporter publisherLimitReporter;
    private final ClientLiveness clientLiveness;

    private final DataHeaderFlyweight heartbeatHeader = new DataHeaderFlyweight();

    private final int positionBitsToShift;
    private final int initialTermId;
    private final EventLogger logger;
    private final SystemCounters systemCounters;
    private final AtomicInteger status = new AtomicInteger(ACTIVE);
    private final int termWindowSize;
    private final InetSocketAddress dstAddress;

    private final AvailabilityHandler sendTransmissionUnitFunc;
    private final AvailabilityHandler onSendRetransmitFunc;

    private int activeIndex = 0;
    private int retransmitIndex = 0;
    private int statusMessagesReceivedCount = 0;

    private long timeOfLastSendOrHeartbeat;
    private long sentPosition = 0;
    private long timeOfFlush = 0;

    private int lastSentTermId;
    private int lastSentTermOffset;
    private int lastSentLength;

    public DriverPublication(final SendChannelEndpoint mediaEndpoint,
                             final TimerWheel timerWheel,
                             final TermBuffers termBuffers,
                             final PositionReporter publisherLimitReporter,
                             final ClientLiveness clientLiveness,
                             final int sessionId,
                             final int streamId,
                             final int initialTermId,
                             final int headerLength,
                             final int mtuLength,
                             final long initialPositionLimit,
                             final EventLogger logger,
                             final SystemCounters systemCounters)
    {
        this.mediaEndpoint = mediaEndpoint;
        this.termBuffers = termBuffers;
        this.logger = logger;
        this.systemCounters = systemCounters;
        this.dstAddress = mediaEndpoint.udpChannel().remoteData();
        this.timerWheel = timerWheel;
        this.publisherLimitReporter = publisherLimitReporter;
        this.clientLiveness = clientLiveness;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.headerLength = headerLength;
        this.mtuLength = mtuLength;
        this.activeIndex = termIdToBufferIndex(initialTermId);

        final RawLog[] rawLogs = termBuffers.buffers();
        for (int i = 0; i < rawLogs.length; i++)
        {
            logScanners[i] = newScanner(rawLogs[i]);
            retransmitLogScanners[i] = newScanner(rawLogs[i]);
            sendBuffers[i] = duplicateLogBuffer(rawLogs[i]);
        }

        final int termCapacity = logScanners[0].capacity();
        positionLimit = new AtomicLong(initialPositionLimit);
        activeTermId = new AtomicInteger(initialTermId);

        timeOfLastSendOrHeartbeat = timerWheel.now();

        this.positionBitsToShift = Integer.numberOfTrailingZeros(termCapacity);
        this.initialTermId = initialTermId;
        termWindowSize = Configuration.publicationTermWindowSize(termCapacity);
        publisherLimitReporter.position(termWindowSize);

        sendTransmissionUnitFunc = this::onSendTransmissionUnit;
        onSendRetransmitFunc = this::onSendRetransmit;

        lastSentTermId = initialTermId;
        lastSentTermOffset = 0;
        lastSentLength = 0;
    }

    public void close()
    {
        termBuffers.close();
        publisherLimitReporter.close();
    }

    public int send()
    {
        int workCount = 0;

        if (status() != FLUSHED)
        {
            final int availableWindow = (int)(positionLimit.get() - sentPosition);
            final int scanLimit = Math.min(availableWindow, mtuLength);

            LogScanner scanner = logScanners[activeIndex];
            workCount += scanner.scanNext(sendTransmissionUnitFunc, scanLimit);

            if (scanner.isComplete())
            {
                activeIndex = TermHelper.rotateNext(activeIndex);
                activeTermId.lazySet(activeTermId.get() + 1);
                scanner = logScanners[activeIndex];
                scanner.seek(0);
            }

            final long position = positionForActiveTerm(scanner.offset());
            sentPosition = position;
            publisherLimitReporter.position(position + termWindowSize);

            if (0 == workCount)
            {
                heartbeatCheck();
            }
        }

        return workCount;
    }

    public SendChannelEndpoint sendChannelEndpoint()
    {
        return mediaEndpoint;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int streamId()
    {
        return streamId;
    }

    public void updatePositionLimitFromStatusMessage(final long limit)
    {
        positionLimit.lazySet(limit);
        statusMessagesReceivedCount++;
    }

    /**
     * This is performed on the {@link DriverConductor} thread
     */
    public int cleanLogBuffer()
    {
        for (final LogBuffer logBuffer : logScanners)
        {
            if (logBuffer.status() == NEEDS_CLEANING && logBuffer.compareAndSetStatus(NEEDS_CLEANING, IN_CLEANING))
            {
                logBuffer.clean();

                return 1;
            }
        }

        return 0;
    }

    public long timeOfLastKeepaliveFromClient()
    {
        return clientLiveness.timeOfLastKeepalive();
    }

    public int status()
    {
        return status.get();
    }

    public void status(final int status)
    {
        this.status.lazySet(status);
    }

    public boolean isFlushed()
    {
        return status() != ACTIVE && logScanners[activeIndex].remaining() == 0;
    }

    public int statusMessagesSeenCount()
    {
        return statusMessagesReceivedCount;
    }

    public void timeOfFlush(final long timestamp)
    {
        timeOfFlush = timestamp;
    }

    public long timeOfFlush()
    {
        return timeOfFlush;
    }

    public void onRetransmit(final int termId, final int termOffset, final int length)
    {
        retransmitIndex = determineIndexByTermId(termId);

        if (-1 != retransmitIndex)
        {
            final LogScanner scanner = retransmitLogScanners[retransmitIndex];
            scanner.seek(termOffset);

            int remainingBytes = length;
            int sent;
            do
            {
                sent = scanner.scanNext(onSendRetransmitFunc, Math.min(remainingBytes, mtuLength));
                remainingBytes -= sent;
            }
            while (remainingBytes > 0 && sent > 0);

            systemCounters.retransmitsSent().orderedIncrement();
        }
    }

    private boolean heartbeatCheck()
    {
        final long timeout = statusMessagesReceivedCount > 0 ? HEARTBEAT_TIMEOUT_NS : INITIAL_HEARTBEAT_TIMEOUT_NS;

        if (timeOfLastSendOrHeartbeat + timeout < timerWheel.now())
        {
            sendHeartbeat();
            return true;
        }

        return false;
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

    private int determineIndexByTermId(final int termId)
    {
        if (termId == activeTermId.get())
        {
            return activeIndex;
        }
        else if (termId == activeTermId.get() - 1)
        {
            return TermHelper.rotatePrevious(activeIndex);
        }

        return -1;
    }

    private void onSendTransmissionUnit(final AtomicBuffer buffer, final int offset, final int length)
    {
        // at this point sendBuffer wraps the same underlying
        // ByteBuffer as the buffer parameter
        final ByteBuffer sendBuffer = sendBuffers[activeIndex];
        sendBuffer.limit(offset + length);
        sendBuffer.position(offset);

        try
        {
            final int bytesSent = mediaEndpoint.sendTo(sendBuffer, dstAddress);
            if (length != bytesSent)
            {
                logger.log(EventCode.FRAME_OUT_INCOMPLETE_SENDTO, "onSendTransmissionUnit %d/%d", bytesSent, length);
            }

            updateTimeOfLastSendOrHeartbeat(timerWheel.now());
            lastSentTermId = activeTermId.get();
            lastSentTermOffset = offset;
            lastSentLength = length;
        }
        catch (final Exception ex)
        {
            logger.logException(ex);
        }
    }

    private void onSendRetransmit(final AtomicBuffer buffer, final int offset, final int length)
    {
        final ByteBuffer termRetransmitBuffer = sendBuffers[retransmitIndex];
        termRetransmitBuffer.limit(offset + length);
        termRetransmitBuffer.position(offset);

        try
        {
            final int bytesSent = mediaEndpoint.sendTo(termRetransmitBuffer, dstAddress);
            if (bytesSent != length)
            {
                logger.log(EventCode.FRAME_OUT_INCOMPLETE_SENDTO, "onSendRetransmit %d/%d", bytesSent, length);
            }
        }
        catch (final Exception ex)
        {
            logger.logException(ex);
        }
    }

    private void sendHeartbeat()
    {
        if (initialTermId == lastSentTermId && 0 == lastSentTermOffset)
        {
            sendZeroLengthDataFrame();
        }
        else
        {
            retransmitIndex = determineIndexByTermId(lastSentTermId);

            if (-1 != retransmitIndex)
            {
                final LogScanner scanner = retransmitLogScanners[retransmitIndex];
                scanner.seek(lastSentTermOffset);

                scanner.scanNext(onSendRetransmitFunc, Math.min(lastSentLength, mtuLength));

                updateTimeOfLastSendOrHeartbeat(timerWheel.now());
            }
        }
    }

    private void sendZeroLengthDataFrame()
    {
        // called from conductor thread on timeout
        // used for both initial setup 0 length data as well as heartbeats
        // send 0 length data frame with current termOffset
        heartbeatHeader.wrap(scratchAtomicBuffer, 0);

        heartbeatHeader.sessionId(sessionId)
                  .streamId(streamId)
                  .termId(activeTermId.get())
                  .termOffset(logScanners[activeIndex].offset())
                  .frameLength(DataHeaderFlyweight.HEADER_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        scratchByteBuffer.position(0);
        scratchByteBuffer.limit(DataHeaderFlyweight.HEADER_LENGTH);

        try
        {
            final int bytesSent = mediaEndpoint.sendTo(scratchByteBuffer, dstAddress);
            systemCounters.heartbeatsSent().orderedIncrement();

            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                logger.log(EventCode.FRAME_OUT_INCOMPLETE_SENDTO, "sendHeartbeat %d/%d",
                    bytesSent, DataHeaderFlyweight.HEADER_LENGTH);
            }

            updateTimeOfLastSendOrHeartbeat(timerWheel.now());
        }
        catch (final Exception ex)
        {
            logger.logException(ex);
        }
    }

    private long positionForActiveTerm(final int termOffset)
    {
        return TermHelper.calculatePosition(activeTermId.get(), termOffset, positionBitsToShift, initialTermId);
    }

    private void updateTimeOfLastSendOrHeartbeat(final long time)
    {
        timeOfLastSendOrHeartbeat = time;
    }
}
