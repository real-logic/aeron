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
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.NanoClock;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogScanner;

import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.SetupFlyweight;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.common.TermHelper.termIdToBufferIndex;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.IN_CLEANING;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.NEEDS_CLEANING;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogScanner.AvailabilityHandler;

/**
 * Publication to be sent to registered subscribers.
 */
public class DriverPublication implements AutoCloseable
{
    private final long id;

    private final NanoClock clock;
    private final int sessionId;
    private final int streamId;
    private final int headerLength;
    private final int mtuLength;

    private final SetupFlyweight setupHeader = new SetupFlyweight();
    private final ByteBuffer setupFrameBuffer = ByteBuffer.allocateDirect(SetupFlyweight.HEADER_LENGTH);

    private final LogScanner[] logScanners = new LogScanner[TermHelper.BUFFER_COUNT];
    private final LogScanner[] retransmitLogScanners = new LogScanner[TermHelper.BUFFER_COUNT];
    private final ByteBuffer[] sendBuffers = new ByteBuffer[TermHelper.BUFFER_COUNT];

    private final AtomicLong senderLimit;
    private final PositionReporter publisherLimit;
    private final SendChannelEndpoint channelEndpoint;
    private final TermBuffers termBuffers;
    private final int positionBitsToShift;
    private final int initialTermId;
    private final EventLogger logger;
    private final SystemCounters systemCounters;
    private final int termWindowSize;
    private final int termCapacity;
    private final InetSocketAddress dstAddress;

    private final AvailabilityHandler sendTransmissionUnitFunc;
    private final AvailabilityHandler onSendRetransmitFunc;

    private volatile boolean shouldSendSetupFrame = true;
    private volatile boolean isActive = true;
    private int activeTermId;
    private int activeIndex = 0;
    private int retransmitIndex = 0;
    private int statusMessagesReceivedCount = 0;

    private long timeOfLastSendOrHeartbeat;
    private long lastSentPosition = 0;
    private long timeOfFlush = 0;

    private int lastSentTermId;
    private int lastSentTermOffset;
    private int lastSentLength;
    private int refCount = 0;

    public DriverPublication(
        final long id,
        final SendChannelEndpoint channelEndpoint,
        final NanoClock clock,
        final TermBuffers termBuffers,
        final PositionReporter publisherLimit,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int headerLength,
        final int mtuLength,
        final long initialPositionLimit,
        final EventLogger logger,
        final SystemCounters systemCounters)
    {
        this.id = id;
        this.channelEndpoint = channelEndpoint;
        this.termBuffers = termBuffers;
        this.logger = logger;
        this.systemCounters = systemCounters;
        this.dstAddress = channelEndpoint.udpChannel().remoteData();
        this.clock = clock;
        this.publisherLimit = publisherLimit;
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
        senderLimit = new AtomicLong(initialPositionLimit);
        activeTermId = initialTermId;

        timeOfLastSendOrHeartbeat = clock.time();

        this.positionBitsToShift = Integer.numberOfTrailingZeros(termCapacity);
        this.initialTermId = initialTermId;
        termWindowSize = Configuration.publicationTermWindowSize(termCapacity);
        publisherLimit.position(termWindowSize);

        sendTransmissionUnitFunc = this::onSendTransmissionUnit;
        onSendRetransmitFunc = this::onSendRetransmit;

        lastSentTermId = initialTermId;
        lastSentTermOffset = 0;
        lastSentLength = 0;

        setupHeader.wrap(new AtomicBuffer(setupFrameBuffer), 0);
        constructSetupFrame();
    }

    public long id()
    {
        return id;
    }

    public void close()
    {
        termBuffers.close();
        publisherLimit.close();
    }

    public int send()
    {
        int bytesSent = 0;

        if (isActive)
        {
            if (shouldSendSetupFrame)
            {
                setupFrameCheck(clock.time());
            }

            bytesSent = sendData();

            if (0 == bytesSent)
            {
                heartbeatCheck(clock.time());
            }
        }

        return bytesSent;
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
        statusMessagesReceivedCount++;
        senderLimit.lazySet(limit);
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

    public void triggerSendSetupFrame()
    {
        shouldSendSetupFrame = true;
    }

    public int decRef()
    {
        return --refCount;
    }

    public int incRef()
    {
        final int i = ++refCount;

        if (i == 1)
        {
            timeOfFlush = 0;
            isActive = true;
        }

        return i;
    }

    public boolean isUnreferencedAndFlushed(final long now)
    {
        final boolean isFlushed = refCount == 0 && logScanners[activeIndex].remaining() == 0;

        if (isFlushed && isActive)
        {
            timeOfFlush = now;
            isActive = false;
        }

        return isFlushed;
    }

    public int initialTermId()
    {
        return initialTermId;
    }

    public TermBuffers termBuffers()
    {
        return termBuffers;
    }

    public int publisherLimitCounterId()
    {
        return publisherLimit.id();
    }

    private int sendData()
    {
        final int bytesSent;
        final int availableWindow = (int)(senderLimit.get() - lastSentPosition);
        final int scanLimit = Math.min(availableWindow, mtuLength);

        LogScanner scanner = logScanners[activeIndex];
        scanner.scanNext(sendTransmissionUnitFunc, scanLimit);

        if (scanner.isComplete())
        {
            activeIndex = TermHelper.rotateNext(activeIndex);
            activeTermId++;
            scanner = logScanners[activeIndex];
            scanner.seek(0);
        }

        final long position = positionForActiveTerm(scanner.offset());
        bytesSent = (int)(position - lastSentPosition);

        lastSentPosition = position;
        publisherLimit.position(position + termWindowSize);

        return bytesSent;
    }

    private void sendSetupFrame(final long now)
    {
        setupHeader.termId(activeTermId);
        setupHeader.termOffset(lastSentTermOffset + lastSentLength);

        setupFrameBuffer.limit(setupHeader.frameLength());
        setupFrameBuffer.position(0);

        final int bytesSent = channelEndpoint.sendTo(setupFrameBuffer, dstAddress);
        if (setupHeader.frameLength() != bytesSent)
        {
            logger.logIncompleteSend("sendSetupFrame", bytesSent, setupHeader.frameLength());
        }

        timeOfLastSendOrHeartbeat = now;
    }

    private void setupFrameCheck(final long now)
    {
        if (0 != lastSentLength || (now > (timeOfLastSendOrHeartbeat + Configuration.PUBLICATION_SETUP_TIMEOUT_NS)))
        {
            sendSetupFrame(now);
        }

        if (statusMessagesReceivedCount > 0)
        {
            shouldSendSetupFrame = false;
        }
    }

    private void heartbeatCheck(final long now)
    {
        if (now > (timeOfLastSendOrHeartbeat + Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS))
        {
            sendHeartbeat(now);
        }
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
        final int activeTermId = this.activeTermId;
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
        final ByteBuffer sendBuffer = sendBuffers[activeIndex];
        sendBuffer.limit(offset + length);
        sendBuffer.position(offset);

        final int bytesSent = channelEndpoint.sendTo(sendBuffer, dstAddress);
        if (length != bytesSent)
        {
            logger.logIncompleteSend("onSendTransmissionUnit", bytesSent, length);
        }

        lastSentTermId = activeTermId;
        lastSentTermOffset = offset;
        lastSentLength = length;
        timeOfLastSendOrHeartbeat = clock.time();
    }

    private void onSendRetransmit(final AtomicBuffer buffer, final int offset, final int length)
    {
        final ByteBuffer termRetransmitBuffer = sendBuffers[retransmitIndex];
        termRetransmitBuffer.limit(offset + length);
        termRetransmitBuffer.position(offset);

        final int bytesSent = channelEndpoint.sendTo(termRetransmitBuffer, dstAddress);
        if (bytesSent != length)
        {
            logger.logIncompleteSend("onSendTransmit", bytesSent, length);
        }
    }

    private void sendHeartbeat(final long now)
    {
        retransmitIndex = determineIndexByTermId(lastSentTermId);

        if (-1 != retransmitIndex)
        {
            final LogScanner scanner = retransmitLogScanners[retransmitIndex];
            scanner.seek(lastSentTermOffset);
            scanner.scanNext(onSendRetransmitFunc, Math.min(lastSentLength, mtuLength));

            systemCounters.heartbeatsSent().orderedIncrement();
            timeOfLastSendOrHeartbeat = now;
        }
    }

    private void constructSetupFrame()
    {
        setupHeader.sessionId(sessionId)
                   .streamId(streamId)
                   .termId(activeTermId)
                   .termOffset(0)
                   .termSize(termCapacity)
                   .frameLength(SetupFlyweight.HEADER_LENGTH)
                   .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
                   .flags((byte)0)
                   .version(HeaderFlyweight.CURRENT_VERSION);
    }

    private long positionForActiveTerm(final int termOffset)
    {
        return TermHelper.calculatePosition(activeTermId, termOffset, positionBitsToShift, initialTermId);
    }
}
