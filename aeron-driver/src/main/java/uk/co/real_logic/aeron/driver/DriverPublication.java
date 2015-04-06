/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
import uk.co.real_logic.aeron.common.protocol.*;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferPartition;
import uk.co.real_logic.agrona.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.RawLog;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * Publication to be sent to registered subscribers.
 */
public class DriverPublication implements AutoCloseable
{
    private final RawLog rawLog;
    private final NanoClock clock;
    private final SetupFlyweight setupHeader = new SetupFlyweight();
    private final ByteBuffer setupFrameBuffer = ByteBuffer.allocateDirect(SetupFlyweight.HEADER_LENGTH);
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final ByteBuffer heartbeatFrameBuffer = ByteBuffer.allocateDirect(DataHeaderFlyweight.HEADER_LENGTH);
    private final TermScanner scanner;
    private final LogBufferPartition[] logPartitions;
    private final ByteBuffer[] sendBuffers;
    private final PositionReporter publisherLimit;
    private final PositionReporter senderPosition;
    private final SendChannelEndpoint channelEndpoint;
    private final InetSocketAddress dstAddress;
    private final SystemCounters systemCounters;

    private final int positionBitsToShift;
    private final int initialTermId;
    private final int termLengthMask;
    private final int mtuLength;
    private final int termWindowLength;

    private long timeOfFlush = 0;
    private long timeOfLastSendOrHeartbeat;
    private int statusMessagesReceivedCount = 0;
    private int refCount = 0;

    private boolean trackSenderLimits = true;
    private volatile long senderLimit;
    private volatile boolean isActive = true;
    private volatile boolean shouldSendSetupFrame = true;

    public DriverPublication(
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
        this.channelEndpoint = channelEndpoint;
        this.rawLog = rawLog;
        this.senderPosition = senderPosition;
        this.systemCounters = systemCounters;
        this.dstAddress = channelEndpoint.udpChannel().remoteData();
        this.clock = clock;
        this.publisherLimit = publisherLimit;
        this.mtuLength = mtuLength;

        logPartitions = rawLog
            .stream()
            .map((partition) -> new LogBufferPartition(partition.termBuffer(), partition.metaDataBuffer()))
            .toArray(LogBufferPartition[]::new);

        scanner = new TermScanner(headerLength);
        sendBuffers = rawLog.sliceTerms();

        final int termLength = logPartitions[0].termBuffer().capacity();
        termLengthMask = termLength - 1;
        senderLimit = initialPositionLimit;

        timeOfLastSendOrHeartbeat = clock.time();

        positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        this.initialTermId = initialTermId;
        termWindowLength = Configuration.publicationTermWindowLength(termLength);
        publisherLimit.position(termWindowLength);

        setupHeader.wrap(new UnsafeBuffer(setupFrameBuffer), 0);
        initSetupFrame(initialTermId, termLength, sessionId, streamId);

        dataHeader.wrap(new UnsafeBuffer(heartbeatFrameBuffer), 0);
        initHeartBeatFrame(sessionId, streamId);
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
            final int termOffset = (int)senderPosition & termLengthMask;
            final long now = clock.time();

            if (shouldSendSetupFrame)
            {
                setupMessageCheck(now, activeTermId, termOffset, senderPosition);
            }

            bytesSent = sendData(now, senderPosition, termOffset);

            if (0 == bytesSent)
            {
                heartbeatMessageCheck(now, senderPosition, activeTermId);
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
        return dataHeader.sessionId();
    }

    public int streamId()
    {
        return dataHeader.streamId();
    }

    public void updatePositionLimitFromStatusMessage(final long limit)
    {
        statusMessagesReceivedCount++;
        senderLimit = limit;
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
            int bytesSent = 0;
            do
            {
                termOffset += bytesSent;

                final int available = scanner.scanForAvailability(termBuffer, termOffset, mtuLength);
                if (available <= 0)
                {
                    break;
                }

                sendBuffer.limit(termOffset + available).position(termOffset);

                if (available != channelEndpoint.sendTo(sendBuffer, dstAddress))
                {
                    systemCounters.dataFrameShortSends().orderedIncrement();
                    break;
                }

                bytesSent += available + scanner.padding();
                remainingBytes -= bytesSent;
            }
            while (remainingBytes > 0);

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
        boolean isFlushed = false;
        if (0 == refCount)
        {
            final long senderPosition = this.senderPosition.position();
            final int activeIndex = indexByPosition(senderPosition, positionBitsToShift);
            isFlushed = (int)(senderPosition & termLengthMask) >= logPartitions[activeIndex].tailVolatile();

            if (isFlushed && isActive)
            {
                timeOfFlush = now;
                isActive = false;
            }
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
        int workCount = 0;
        final long candidatePublisherLimit = senderPosition.position() + termWindowLength;
        if (publisherLimit.position() != candidatePublisherLimit)
        {
            publisherLimit.position(candidatePublisherLimit);
            workCount = 1;
        }

        return workCount;
    }

    private int sendData(final long now, final long senderPosition, final int termOffset)
    {
        int bytesSent = 0;
        final int availableWindow = (int)(senderLimit - senderPosition);
        if (availableWindow > 0)
        {
            final int scanLimit = Math.min(availableWindow, mtuLength);
            final int activeIndex = indexByPosition(senderPosition, positionBitsToShift);

            final int available = scanner.scanForAvailability(logPartitions[activeIndex].termBuffer(), termOffset, scanLimit);
            if (available > 0)
            {
                final ByteBuffer sendBuffer = sendBuffers[activeIndex];
                sendBuffer.limit(termOffset + available).position(termOffset);

                if (available == channelEndpoint.sendTo(sendBuffer, dstAddress))
                {
                    timeOfLastSendOrHeartbeat = now;
                    trackSenderLimits = true;

                    bytesSent = available;
                    this.senderPosition.position(senderPosition + bytesSent + scanner.padding());
                }
                else
                {
                    systemCounters.dataFrameShortSends().orderedIncrement();
                }
            }
        }
        else if (trackSenderLimits)
        {
            trackSenderLimits = false;
            systemCounters.senderFlowControlLimits().orderedIncrement();
        }

        return bytesSent;
    }

    private void setupMessageCheck(final long now, final int activeTermId, final int termOffset, final long senderPosition)
    {
        if (0 != senderPosition || (now > (timeOfLastSendOrHeartbeat + Configuration.PUBLICATION_SETUP_TIMEOUT_NS)))
        {
            setupHeader.activeTermId(activeTermId).termOffset(termOffset);

            final int frameLength = setupHeader.frameLength();
            setupFrameBuffer.limit(frameLength).position(0);

            final int bytesSent = channelEndpoint.sendTo(setupFrameBuffer, dstAddress);
            if (frameLength != bytesSent)
            {
                systemCounters.setupFrameShortSends().orderedIncrement();
            }

            timeOfLastSendOrHeartbeat = now;
        }

        if (statusMessagesReceivedCount > 0)
        {
            shouldSendSetupFrame = false;
        }
    }

    private void heartbeatMessageCheck(final long now, final long senderPosition, final int activeTermId)
    {
        if (now > (timeOfLastSendOrHeartbeat + Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS))
        {
            final int termOffset = (int)senderPosition & termLengthMask;

            heartbeatFrameBuffer.clear();
            dataHeader.termOffset(termOffset).termId(activeTermId);

            final int bytesSent = channelEndpoint.sendTo(heartbeatFrameBuffer, dstAddress);
            if (bytesSent != DataHeaderFlyweight.HEADER_LENGTH)
            {
                systemCounters.dataFrameShortSends().orderedIncrement();
            }

            systemCounters.heartbeatsSent().orderedIncrement();
            timeOfLastSendOrHeartbeat = now;
        }
    }

    private void initSetupFrame(final int activeTermId, final int termLength, final int sessionId, final int streamId)
    {
        setupHeader
            .sessionId(sessionId)
            .streamId(streamId)
            .initialTermId(initialTermId)
            .activeTermId(activeTermId)
            .termOffset(0)
            .termLength(termLength)
            .mtuLength(mtuLength)
            .frameLength(SetupFlyweight.HEADER_LENGTH)
            .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
            .flags((byte)0)
            .version(HeaderFlyweight.CURRENT_VERSION);
    }

    private void initHeartBeatFrame(final int sessionId, final int streamId)
    {
        dataHeader
            .sessionId(sessionId)
            .streamId(streamId)
            .frameLength(0)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .flags((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
    }
}
