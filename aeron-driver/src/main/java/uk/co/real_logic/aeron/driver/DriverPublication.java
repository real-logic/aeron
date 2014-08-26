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
import uk.co.real_logic.aeron.common.protocol.SetupFlyweight;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.common.TermHelper.termIdToBufferIndex;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogScanner.AvailabilityHandler;

/**
 * Publication to be sent to registered subscribers.
 */
public class DriverPublication implements AutoCloseable
{
    public enum Status {ACTIVE, EOF, FLUSHED}

    private final TimerWheel timerWheel;
    private final int sessionId;
    private final int streamId;
    private final AtomicInteger activeTermId;
    private final int headerLength;
    private final int mtuLength;

    private final ByteBuffer setupFrameBuffer = ByteBuffer.allocateDirect(DataHeaderFlyweight.HEADER_LENGTH);
    private final LogScanner[] logScanners = new LogScanner[TermHelper.BUFFER_COUNT];
    private final LogScanner[] retransmitLogScanners = new LogScanner[TermHelper.BUFFER_COUNT];
    private final ByteBuffer[] sendBuffers = new ByteBuffer[TermHelper.BUFFER_COUNT];

    private final AtomicLong positionLimit;
    private final SendChannelEndpoint channelEndpoint;
    private final TermBuffers termBuffers;
    private final PositionReporter publisherLimitReporter;
    private final AeronClient aeronClient;

    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private final int positionBitsToShift;
    private final int initialTermId;
    private final EventLogger logger;
    private final SystemCounters systemCounters;
    private final int termWindowSize;
    private final int termCapacity;
    private final InetSocketAddress dstAddress;

    private final AvailabilityHandler sendTransmissionUnitFunc;
    private final AvailabilityHandler onSendRetransmitFunc;

    private volatile Status status = Status.ACTIVE;
    private int activeIndex = 0;
    private int retransmitIndex = 0;
    private int statusMessagesReceivedCount = 0;

    private long timeOfLastSendOrHeartbeat;
    private long sentPosition = 0;
    private long timeOfFlush = 0;

    private int lastSentTermId;
    private int lastSentTermOffset;
    private int lastSentLength;

    public DriverPublication(
        final SendChannelEndpoint channelEndpoint,
        final TimerWheel timerWheel,
        final TermBuffers termBuffers,
        final PositionReporter publisherLimitReporter,
        final AeronClient aeronClient,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int headerLength,
        final int mtuLength,
        final long initialPositionLimit,
        final EventLogger logger,
        final SystemCounters systemCounters)
    {
        this.channelEndpoint = channelEndpoint;
        this.termBuffers = termBuffers;
        this.logger = logger;
        this.systemCounters = systemCounters;
        this.dstAddress = channelEndpoint.udpChannel().remoteData();
        this.timerWheel = timerWheel;
        this.publisherLimitReporter = publisherLimitReporter;
        this.aeronClient = aeronClient;
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

        termCapacity = logScanners[0].capacity();
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

        setupHeader.wrap(new AtomicBuffer(setupFrameBuffer), 0);
        constructSetupFrame();
    }

    public void close()
    {
        termBuffers.close();
        publisherLimitReporter.close();
    }

    public int send()
    {
        int workCount = 0;

        if (status != Status.FLUSHED)
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
        return channelEndpoint;
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
        return aeronClient.timeOfLastKeepalive();
    }

    public Status status()
    {
        return status;
    }

    public void status(final Status status)
    {
        this.status = status;
    }

    public boolean isFlushed()
    {
        return status != Status.ACTIVE && logScanners[activeIndex].remaining() == 0;
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

    // called from either Sender thread (initial setup) or Conductor thread (in response to SEND_SETUP_FLAG in SMs)
    public void sendSetupFrame()
    {
        setupHeader.termId(activeTermId.get()); // update the termId field

        setupFrameBuffer.limit(setupHeader.frameLength());
        setupFrameBuffer.position(0);

        final int bytesSent = channelEndpoint.sendTo(setupFrameBuffer, dstAddress);
        systemCounters.heartbeatsSent().orderedIncrement();

        if (setupHeader.frameLength() != bytesSent)
        {
            logger.log(
                EventCode.FRAME_OUT_INCOMPLETE_SEND,
                "sendSetupFrame %d/%d",
                bytesSent,
                setupHeader.frameLength());
        }

        updateTimeOfLastSendOrSetup(timerWheel.now());
    }

    private boolean heartbeatCheck()
    {
        final long timeout =
            statusMessagesReceivedCount > 0 ?
                Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS :
                Configuration.PUBLICATION_SETUP_TIMEOUT_NS;

        if (timeOfLastSendOrHeartbeat + timeout < timerWheel.now())
        {
            sendSetupFrameOrHeartbeat();
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
        final int activeTermId = this.activeTermId.get();
        if (termId == activeTermId)
        {
            return activeIndex;
        }
        else if (termId == activeTermId - 1)
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

        final int bytesSent = channelEndpoint.sendTo(sendBuffer, dstAddress);
        if (length != bytesSent)
        {
            logger.log(EventCode.FRAME_OUT_INCOMPLETE_SEND, "onSendTransmissionUnit %d/%d", bytesSent, length);
        }

        updateTimeOfLastSendOrSetup(timerWheel.now());
        lastSentTermId = activeTermId.get();
        lastSentTermOffset = offset;
        lastSentLength = length;
    }

    private void onSendRetransmit(final AtomicBuffer buffer, final int offset, final int length)
    {
        final ByteBuffer termRetransmitBuffer = sendBuffers[retransmitIndex];
        termRetransmitBuffer.limit(offset + length);
        termRetransmitBuffer.position(offset);

        final int bytesSent = channelEndpoint.sendTo(termRetransmitBuffer, dstAddress);
        if (bytesSent != length)
        {
            logger.log(EventCode.FRAME_OUT_INCOMPLETE_SEND, "onSendRetransmit %d/%d", bytesSent, length);
        }
    }

    private void sendSetupFrameOrHeartbeat()
    {
        if (0 == lastSentLength && 0 == lastSentTermOffset && initialTermId == lastSentTermId)
        {
            sendSetupFrame();
        }
        else
        {
            retransmitIndex = determineIndexByTermId(lastSentTermId);

            if (-1 != retransmitIndex)
            {
                final LogScanner scanner = retransmitLogScanners[retransmitIndex];
                scanner.seek(lastSentTermOffset);

                scanner.scanNext(onSendRetransmitFunc, Math.min(lastSentLength, mtuLength));

                updateTimeOfLastSendOrSetup(timerWheel.now());
            }
        }
    }

    private void constructSetupFrame()
    {
        setupHeader.sessionId(sessionId)
                   .streamId(streamId)
                   .termId(activeTermId.get())
                   .termSize(termCapacity)
                   .frameLength(SetupFlyweight.HEADER_LENGTH)
                   .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
                   .flags((byte) 0)
                   .version(HeaderFlyweight.CURRENT_VERSION);
    }

    private long positionForActiveTerm(final int termOffset)
    {
        return TermHelper.calculatePosition(activeTermId.get(), termOffset, positionBitsToShift, initialTermId);
    }

    private void updateTimeOfLastSendOrSetup(final long time)
    {
        timeOfLastSendOrHeartbeat = time;
    }
}
