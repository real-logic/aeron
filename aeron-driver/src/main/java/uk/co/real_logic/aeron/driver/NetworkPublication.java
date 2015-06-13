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

import uk.co.real_logic.aeron.logbuffer.LogBufferPartition;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.protocol.SetupFlyweight;
import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.driver.media.SendChannelEndpoint;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.driver.Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS;
import static uk.co.real_logic.aeron.driver.Configuration.PUBLICATION_SETUP_TIMEOUT_NS;
import static uk.co.real_logic.aeron.logbuffer.TermScanner.available;
import static uk.co.real_logic.aeron.logbuffer.TermScanner.padding;
import static uk.co.real_logic.aeron.logbuffer.TermScanner.scanForAvailability;

/**
 * Publication to be sent to registered subscribers.
 */
public class NetworkPublication implements RetransmitSender, AutoCloseable
{
    private final RawLog rawLog;
    private final NanoClock clock;
    private final SetupFlyweight setupHeader = new SetupFlyweight();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final ByteBuffer setupFrameBuffer = ByteBuffer.allocateDirect(SetupFlyweight.HEADER_LENGTH);
    private final ByteBuffer heartbeatFrameBuffer = ByteBuffer.allocateDirect(DataHeaderFlyweight.HEADER_LENGTH);
    private final LogBufferPartition[] logPartitions;
    private final ByteBuffer[] sendBuffers;
    private final Position publisherLimit;
    private final Position senderPosition;
    private final SendChannelEndpoint channelEndpoint;
    private final InetSocketAddress dstAddress;
    private final SystemCounters systemCounters;

    private final int positionBitsToShift;
    private final int initialTermId;
    private final int termLengthMask;
    private final int mtuLength;
    private final int termWindowLength;

    private long timeOfLastSendOrHeartbeat;
    private long timeOfFlush = 0;
    private int statusMessagesReceivedCount = 0;
    private int refCount = 0;

    private volatile long senderPositionLimit;
    private boolean trackSenderLimits = true;
    private volatile boolean isActive = true;
    private volatile boolean shouldSendSetupFrame = true;

    public NetworkPublication(
        final SendChannelEndpoint channelEndpoint,
        final NanoClock clock,
        final RawLog rawLog,
        final Position senderPosition,
        final Position publisherLimit,
        final int sessionId,
        final int streamId,
        final int initialTermId,
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

        sendBuffers = rawLog.sliceTerms();

        final int termLength = logPartitions[0].termBuffer().capacity();
        termLengthMask = termLength - 1;
        senderPositionLimit = initialPositionLimit;

        timeOfLastSendOrHeartbeat = clock.nanoTime();

        positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        this.initialTermId = initialTermId;
        termWindowLength = Configuration.publicationTermWindowLength(termLength);
        publisherLimit.setOrdered(termWindowLength);

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
            final long senderPosition = this.senderPosition.get();
            final int activeTermId = computeTermIdFromPosition(senderPosition, positionBitsToShift, initialTermId);
            final int termOffset = (int)senderPosition & termLengthMask;
            final long now = clock.nanoTime();

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

    public void senderPositionLimit(final long positionLimit)
    {
        statusMessagesReceivedCount++;
        senderPositionLimit = positionLimit;
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

    public void resend(final int termId, int termOffset, final int length)
    {
        final long senderPosition = this.senderPosition.get();
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

                final long scanOutcome = scanForAvailability(termBuffer, termOffset, mtuLength);
                final int available = available(scanOutcome);
                if (available <= 0)
                {
                    break;
                }

                sendBuffer.limit(termOffset + available).position(termOffset);

                if (available != channelEndpoint.sendTo(sendBuffer, dstAddress))
                {
                    systemCounters.dataPacketShortSends().orderedIncrement();
                    break;
                }

                bytesSent = available + padding(scanOutcome);
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
            final long senderPosition = this.senderPosition.getVolatile();
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

    public RawLog rawLog()
    {
        return rawLog;
    }

    public int publisherLimitId()
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
        final long candidatePublisherLimit = senderPosition.getVolatile() + termWindowLength;
        if (publisherLimit.proposeMaxOrdered(candidatePublisherLimit))
        {
            workCount = 1;
        }

        return workCount;
    }

    private int sendData(final long now, final long senderPosition, final int termOffset)
    {
        int bytesSent = 0;
        final int availableWindow = (int)(senderPositionLimit - senderPosition);
        if (availableWindow > 0)
        {
            final int scanLimit = Math.min(availableWindow, mtuLength);
            final int activeIndex = indexByPosition(senderPosition, positionBitsToShift);

            final long scanOutcome = scanForAvailability(logPartitions[activeIndex].termBuffer(), termOffset, scanLimit);
            final int available = available(scanOutcome);
            if (available > 0)
            {
                final ByteBuffer sendBuffer = sendBuffers[activeIndex];
                sendBuffer.limit(termOffset + available).position(termOffset);

                if (available == channelEndpoint.sendTo(sendBuffer, dstAddress))
                {
                    timeOfLastSendOrHeartbeat = now;
                    trackSenderLimits = true;

                    bytesSent = available;
                    this.senderPosition.setOrdered(senderPosition + bytesSent + padding(scanOutcome));
                }
                else
                {
                    systemCounters.dataPacketShortSends().orderedIncrement();
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
        if (0 != senderPosition || (now > (timeOfLastSendOrHeartbeat + PUBLICATION_SETUP_TIMEOUT_NS)))
        {
            setupFrameBuffer.clear();
            setupHeader.activeTermId(activeTermId).termOffset(termOffset);

            final int bytesSent = channelEndpoint.sendTo(setupFrameBuffer, dstAddress);
            if (SetupFlyweight.HEADER_LENGTH != bytesSent)
            {
                systemCounters.setupMessageShortSends().orderedIncrement();
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
        if (now > (timeOfLastSendOrHeartbeat + PUBLICATION_HEARTBEAT_TIMEOUT_NS))
        {
            final int termOffset = (int)senderPosition & termLengthMask;

            heartbeatFrameBuffer.clear();
            dataHeader.termId(activeTermId).termOffset(termOffset);

            final int bytesSent = channelEndpoint.sendTo(heartbeatFrameBuffer, dstAddress);
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                systemCounters.dataPacketShortSends().orderedIncrement();
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
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags((byte)0)
            .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
            .frameLength(SetupFlyweight.HEADER_LENGTH);
    }

    private void initHeartBeatFrame(final int sessionId, final int streamId)
    {
        dataHeader
            .sessionId(sessionId)
            .streamId(streamId)
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(0);
    }
}
