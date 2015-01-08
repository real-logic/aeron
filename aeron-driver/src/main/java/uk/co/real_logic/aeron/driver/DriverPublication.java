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

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferPartition;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogScanner;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.SetupFlyweight;
import uk.co.real_logic.agrona.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.RawLogPartition;
import uk.co.real_logic.aeron.driver.buffer.RawLog;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.common.TermHelper.bufferIndex;
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
    private final PositionReporter senderPosition;

    private final SendChannelEndpoint channelEndpoint;
    private final RawLog rawLog;
    private final int positionBitsToShift;
    private final int initialTermId;
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
    private long timeOfFlush = 0;

    private int lastSentTermId;
    private int lastSentTermOffset;
    private int lastSentLength;
    private int refCount = 0;

    public DriverPublication(
        final long id,
        final SendChannelEndpoint channelEndpoint,
        final NanoClock clock,
        final RawLog rawLog,
        final PositionReporter senderPosition,
        final PositionReporter publisherLimit,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int headerLength,
        final int mtuLength,
        final long initialPositionLimit,
        final SystemCounters systemCounters)
    {
        this.id = id;
        this.channelEndpoint = channelEndpoint;
        this.rawLog = rawLog;
        this.senderPosition = senderPosition;
        this.systemCounters = systemCounters;
        this.dstAddress = channelEndpoint.udpChannel().remoteData();
        this.clock = clock;
        this.publisherLimit = publisherLimit;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.headerLength = headerLength;
        this.mtuLength = mtuLength;
        this.activeIndex = bufferIndex(initialTermId, initialTermId);

        final RawLogPartition[] rawLogPartitions = rawLog.partitions();
        for (int i = 0; i < rawLogPartitions.length; i++)
        {
            logScanners[i] = newScanner(rawLogPartitions[i]);
            retransmitLogScanners[i] = newScanner(rawLogPartitions[i]);
            sendBuffers[i] = duplicateLogBuffer(rawLogPartitions[i]);
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

        setupHeader.wrap(new UnsafeBuffer(setupFrameBuffer), 0);
        constructSetupFrame();
    }

    public long id()
    {
        return id;
    }

    public void close()
    {
        rawLog.close();
        publisherLimit.close();
        senderPosition.close();
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
        int workCount = 0;

        for (final LogBufferPartition logBufferPartition : logScanners)
        {
            if (logBufferPartition.status() == NEEDS_CLEANING)
            {
                logBufferPartition.clean();
                workCount = 1;
            }
        }

        return workCount;
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

    public RawLog rawLogBuffers()
    {
        return rawLog;
    }

    public int publisherLimitCounterId()
    {
        return publisherLimit.id();
    }

    /**
     * Update the publishers limit for flow control as part of the conductor duty cycle.
     *
     * @return 1 if the limit has been updated otherwise 0.
     */
    public int updatePublishersLimit()
    {
        final long candidatePublisherLimit = senderPosition.position() + termWindowSize;
        if (publisherLimit.position() != candidatePublisherLimit)
        {
            publisherLimit.position(candidatePublisherLimit);
            return 1;
        }

        return 0;
    }

    private int sendData()
    {
        final int bytesSent;
        final long lastSentPosition = senderPosition.position();
        final int availableWindow = (int)(senderLimit.get() - lastSentPosition);
        final int scanLimit = Math.min(availableWindow, mtuLength);

        LogScanner scanner = logScanners[activeIndex];
        scanner.scanNext(sendTransmissionUnitFunc, scanLimit);

        if (scanner.isComplete())
        {
            activeIndex = BitUtil.next(activeIndex, TermHelper.BUFFER_COUNT);
            activeTermId++;
            scanner = logScanners[activeIndex];
            scanner.seek(0);
        }

        final long position = TermHelper.calculatePosition(activeTermId, scanner.offset(), positionBitsToShift, initialTermId);
        bytesSent = (int)(position - lastSentPosition);

        senderPosition.position(position);

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
            systemCounters.setupFrameShortSends().orderedIncrement();
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

    private ByteBuffer duplicateLogBuffer(final RawLogPartition log)
    {
        final ByteBuffer buffer = log.termBuffer().duplicateByteBuffer();
        buffer.clear();

        return buffer;
    }

    private LogScanner newScanner(final RawLogPartition log)
    {
        return new LogScanner(log.termBuffer(), log.metaDataBuffer(), headerLength);
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

    private void onSendTransmissionUnit(final UnsafeBuffer buffer, final int offset, final int length)
    {
        final ByteBuffer sendBuffer = sendBuffers[activeIndex];
        sendBuffer.limit(offset + length);
        sendBuffer.position(offset);

        final int bytesSent = channelEndpoint.sendTo(sendBuffer, dstAddress);
        if (length != bytesSent)
        {
            systemCounters.dataFrameShortSends().orderedIncrement();
        }

        lastSentTermId = activeTermId;
        lastSentTermOffset = offset;
        lastSentLength = length;
        timeOfLastSendOrHeartbeat = clock.time();
    }

    private void onSendRetransmit(final UnsafeBuffer buffer, final int offset, final int length)
    {
        final ByteBuffer termRetransmitBuffer = sendBuffers[retransmitIndex];
        termRetransmitBuffer.limit(offset + length);
        termRetransmitBuffer.position(offset);

        final int bytesSent = channelEndpoint.sendTo(termRetransmitBuffer, dstAddress);
        if (bytesSent != length)
        {
            systemCounters.dataFrameShortSends().orderedIncrement();
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
                   .mtuLength(mtuLength)
                   .frameLength(SetupFlyweight.HEADER_LENGTH)
                   .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
                   .flags((byte)0)
                   .version(HeaderFlyweight.CURRENT_VERSION);
    }
}
