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

import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermScanner;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferPartition;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.SetupFlyweight;
import uk.co.real_logic.agrona.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.RawLog;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * Publication to be sent to registered subscribers.
 */
public class DriverPublication implements AutoCloseable
{
    private final RawLog rawLog;
    private final NanoClock clock;
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private final TermScanner scanner;
    private final ByteBuffer setupFrameBuffer = ByteBuffer.allocateDirect(SetupFlyweight.HEADER_LENGTH);
    private final LogBufferPartition[] logPartitions;

    private final ByteBuffer[] sendBuffers;
    private final AtomicLong senderLimit;
    private final PositionReporter publisherLimit;

    private final PositionReporter senderPosition;
    private final SystemCounters systemCounters;
    private final SendChannelEndpoint channelEndpoint;
    private final InetSocketAddress dstAddress;

    private final long id;
    private final int sessionId;
    private final int streamId;

    private final int positionBitsToShift;
    private final int initialTermId;
    private final int termLengthMask;
    private final int termLength;
    private final int mtuLength;
    private final int termWindowLength;

    private long timeOfLastSendOrHeartbeat;
    private long timeOfFlush = 0;
    private int lastSendLength = 0;
    private int statusMessagesReceivedCount = 0;
    private int refCount = 0;

    private volatile boolean isActive = true;
    private volatile boolean shouldSendSetupFrame = true;

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
        this.mtuLength = mtuLength;

        logPartitions = rawLog
            .stream()
            .map((partition) -> new LogBufferPartition(partition.termBuffer(), partition.metaDataBuffer()))
            .toArray(LogBufferPartition[]::new);

        scanner = new TermScanner(headerLength);
        sendBuffers = rawLog.sliceTerms();
        termLength = logPartitions[0].capacity();
        termLengthMask = termLength - 1;
        senderLimit = new AtomicLong(initialPositionLimit);

        timeOfLastSendOrHeartbeat = clock.time();

        this.positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        this.initialTermId = initialTermId;
        termWindowLength = Configuration.publicationTermWindowLength(termLength);
        publisherLimit.position(termWindowLength);

        setupHeader.wrap(new UnsafeBuffer(setupFrameBuffer), 0);
        constructSetupFrame(initialTermId);
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
            final long senderPosition = this.senderPosition.position();
            final int activeTermId = computeTermIdFromPosition(senderPosition, positionBitsToShift, initialTermId);
            final long now = clock.time();

            if (shouldSendSetupFrame)
            {
                setupFrameCheck(now, activeTermId, senderPosition);
            }

            bytesSent = sendData(now, senderPosition);

            if (0 == bytesSent)
            {
                heartbeatCheck(now, senderPosition);
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

        for (final LogBufferPartition partition : logPartitions)
        {
            if (partition.status() == NEEDS_CLEANING)
            {
                partition.clean();
                workCount = 1;
            }
        }

        return workCount;
    }

    public long timeOfFlush()
    {
        return timeOfFlush;
    }

    public void onRetransmit(final int termId, int termOffset, final int length)
    {
        final long senderPosition = this.senderPosition.position();
        final int activeTermId = computeTermIdFromPosition(senderPosition, positionBitsToShift, initialTermId);

        if (termId == activeTermId || termId == (activeTermId - 1))
        {
            final int activeIndex = indexByTerm(initialTermId, termId);
            final UnsafeBuffer termBuffer = logPartitions[activeIndex].termBuffer();
            final ByteBuffer sendBuffer = sendBuffers[activeIndex];

            int remainingBytes = length;
            int totalBytesSent = 0;
            do
            {
                termOffset += totalBytesSent;

                int available = 0;
                if (scanner.scanForAvailability(termBuffer, termOffset, mtuLength))
                {
                    available = scanner.available();
                    sendBuffer.limit(termOffset + available);
                    sendBuffer.position(termOffset);

                    final int bytesSent = channelEndpoint.sendTo(sendBuffer, dstAddress);
                    if (available != bytesSent)
                    {
                        systemCounters.dataFrameShortSends().orderedIncrement();
                    }
                }

                totalBytesSent = available + scanner.padding();
                remainingBytes -= totalBytesSent;
            }
            while (remainingBytes > 0 && totalBytesSent > 0);
        }

        systemCounters.retransmitsSent().orderedIncrement();
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
        boolean isFlushed = false;
        if (refCount == 0)
        {
            final long senderPosition = this.senderPosition.position();
            final int activeIndex = indexByPosition(senderPosition, positionBitsToShift);
            isFlushed = logPartitions[activeIndex].tailVolatile() == (int)(senderPosition & termLengthMask);
        }

        if (isFlushed && isActive)
        {
            timeOfFlush = now;
            isActive = false;
        }

        return isFlushed;
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
        final long candidatePublisherLimit = senderPosition.position() + termWindowLength;
        if (publisherLimit.position() != candidatePublisherLimit)
        {
            publisherLimit.position(candidatePublisherLimit);
            return 1;
        }

        return 0;
    }

    private int sendData(final long now, final long senderPosition)
    {
        final int availableWindow = (int)(senderLimit.get() - senderPosition);
        final int scanLimit = Math.min(availableWindow, mtuLength);
        final int partitionOffset = (int)senderPosition & termLengthMask;
        final int activeIndex = indexByPosition(senderPosition, positionBitsToShift);

        if (scanner.scanForAvailability(logPartitions[activeIndex].termBuffer(), partitionOffset, scanLimit))
        {
            final int available = scanner.available();

            final ByteBuffer sendBuffer = sendBuffers[activeIndex];
            sendBuffer.limit(partitionOffset + available);
            sendBuffer.position(partitionOffset);

            final int bytesSent = channelEndpoint.sendTo(sendBuffer, dstAddress);
            if (available != bytesSent)
            {
                systemCounters.dataFrameShortSends().orderedIncrement();
            }

            lastSendLength = available;
            timeOfLastSendOrHeartbeat = now;

            this.senderPosition.position(senderPosition + available + scanner.padding());
        }

        return scanner.available();
    }

    private void sendSetupFrame(final long now, final int activeTermId, final long position)
    {
        setupHeader.termId(activeTermId);
        setupHeader.termOffset((int)position & termLengthMask);

        setupFrameBuffer.limit(setupHeader.frameLength());
        setupFrameBuffer.position(0);

        final int bytesSent = channelEndpoint.sendTo(setupFrameBuffer, dstAddress);
        if (setupHeader.frameLength() != bytesSent)
        {
            systemCounters.setupFrameShortSends().orderedIncrement();
        }

        timeOfLastSendOrHeartbeat = now;
    }

    private void setupFrameCheck(final long now, final int activeTermId, final long position)
    {
        if (0 != lastSendLength || (now > (timeOfLastSendOrHeartbeat + Configuration.PUBLICATION_SETUP_TIMEOUT_NS)))
        {
            sendSetupFrame(now, activeTermId, position);
        }

        if (statusMessagesReceivedCount > 0)
        {
            shouldSendSetupFrame = false;
        }
    }

    private void heartbeatCheck(final long now, final long senderPosition)
    {
        if (now > (timeOfLastSendOrHeartbeat + Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS))
        {
            sendHeartbeat(now, senderPosition);
        }
    }

    private void sendHeartbeat(final long now, final long senderPosition)
    {
        final int length = lastSendLength;
        final long lastSentPosition = senderPosition - length;
        final int termOffset = (int)lastSentPosition & termLengthMask;
        final int activeIndex = indexByPosition(lastSentPosition, positionBitsToShift);

        final ByteBuffer sendBuffer = sendBuffers[activeIndex];
        sendBuffer.limit(termOffset + length);
        sendBuffer.position(termOffset);

        final int bytesSent = channelEndpoint.sendTo(sendBuffer, dstAddress);
        if (bytesSent != length)
        {
            systemCounters.dataFrameShortSends().orderedIncrement();
        }

        systemCounters.heartbeatsSent().orderedIncrement();
        timeOfLastSendOrHeartbeat = now;
    }

    private void constructSetupFrame(final int activeTermId)
    {
        setupHeader.sessionId(sessionId)
                   .streamId(streamId)
                   .termId(activeTermId)
                   .termOffset(0)
                   .termLength(termLength)
                   .mtuLength(mtuLength)
                   .frameLength(SetupFlyweight.HEADER_LENGTH)
                   .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
                   .flags((byte)0)
                   .version(HeaderFlyweight.CURRENT_VERSION);
    }
}
