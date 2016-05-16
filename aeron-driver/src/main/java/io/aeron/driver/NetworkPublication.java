/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.LogBufferPartition;
import io.aeron.logbuffer.LogBufferUnblocker;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.Position;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.driver.Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS;
import static io.aeron.driver.Configuration.PUBLICATION_SETUP_TIMEOUT_NS;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.logbuffer.TermScanner.*;

class NetworkPublicationPadding1
{
    @SuppressWarnings("unused")
    long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

class NetworkPublicationConductorFields extends NetworkPublicationPadding1
{
    long timeOfLastActivity = 0;
    long lastSenderPosition = 0;
    int refCount = 0;
}

class NetworkPublicationPadding2 extends NetworkPublicationConductorFields
{
    @SuppressWarnings("unused")
    long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

class NetworkPublicationReceiverFields extends NetworkPublicationPadding2
{
    long timeOfLastSendOrHeartbeat;
    long senderPositionLimit = 0;
    long timeOfLastSetup;
    boolean trackSenderLimits = true;
    boolean shouldSendSetupFrame = true;
}

class NetworkPublicationPadding3 extends NetworkPublicationReceiverFields
{
    @SuppressWarnings("unused")
    long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}

/**
 * Publication to be sent to registered subscribers.
 */
public class NetworkPublication
    extends NetworkPublicationPadding3
    implements RetransmitSender, DriverManagedResource
{
    private final int positionBitsToShift;
    private final int initialTermId;
    private final int termLengthMask;
    private final int mtuLength;
    private final int termWindowLength;
    private final int sessionId;
    private final int streamId;

    private volatile boolean hasStatusMessageBeenReceived = false;
    private boolean reachedEndOfLife = false;

    private final LogBufferPartition[] logPartitions;
    private final ByteBuffer[] sendBuffers;
    private final Position publisherLimit;
    private final Position senderPosition;
    private final SendChannelEndpoint channelEndpoint;
    private final ByteBuffer heartbeatBuffer;
    private final DataHeaderFlyweight dataHeader;
    private final ByteBuffer setupBuffer;
    private final SetupFlyweight setupHeader;
    private final FlowControl flowControl;
    private final EpochClock epochClock;
    private final RetransmitHandler retransmitHandler;
    private final RawLog rawLog;
    private final AtomicCounter heartbeatsSent;
    private final AtomicCounter retransmitsSent;
    private final AtomicCounter senderFlowControlLimits;
    private final AtomicCounter shortSends;

    public NetworkPublication(
        final SendChannelEndpoint channelEndpoint,
        final NanoClock nanoClock,
        final EpochClock epochClock,
        final RawLog rawLog,
        final Position publisherLimit,
        final Position senderPosition,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int mtuLength,
        final SystemCounters systemCounters,
        final FlowControl flowControl,
        final RetransmitHandler retransmitHandler,
        final NetworkPublicationThreadLocals threadLocals)
    {
        this.channelEndpoint = channelEndpoint;
        this.rawLog = rawLog;
        this.epochClock = epochClock;
        this.senderPosition = senderPosition;
        this.flowControl = flowControl;
        this.retransmitHandler = retransmitHandler;
        this.publisherLimit = publisherLimit;
        this.mtuLength = mtuLength;
        this.initialTermId = initialTermId;
        this.sessionId = sessionId;
        this.streamId = streamId;

        setupBuffer = threadLocals.setupBuffer();
        setupHeader = threadLocals.setupHeader();
        heartbeatBuffer = threadLocals.heartbeatBuffer();
        dataHeader = threadLocals.dataHeader();

        heartbeatsSent = systemCounters.get(HEARTBEATS_SENT);
        shortSends = systemCounters.get(SHORT_SENDS);
        retransmitsSent = systemCounters.get(RETRANSMITS_SENT);
        senderFlowControlLimits = systemCounters.get(SENDER_FLOW_CONTROL_LIMITS);

        logPartitions = rawLog.partitions();
        sendBuffers = rawLog.sliceTerms();

        final int termLength = rawLog.termLength();
        termLengthMask = termLength - 1;
        flowControl.initialize(initialTermId, termLength);

        final long time = nanoClock.nanoTime();
        timeOfLastSendOrHeartbeat = time - PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1;
        timeOfLastSetup = time - PUBLICATION_SETUP_TIMEOUT_NS - 1;

        positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        termWindowLength = Configuration.publicationTermWindowLength(termLength);
    }

    public void close()
    {
        rawLog.close();
        publisherLimit.close();
        senderPosition.close();
    }

    public int send(final long now)
    {
        final long senderPosition = this.senderPosition.get();
        final int activeTermId = computeTermIdFromPosition(senderPosition, positionBitsToShift, initialTermId);
        final int termOffset = (int)senderPosition & termLengthMask;

        if (shouldSendSetupFrame)
        {
            setupMessageCheck(now, activeTermId, termOffset);
        }

        final int bytesSent = sendData(now, senderPosition, termOffset);

        if (0 == bytesSent)
        {
            heartbeatMessageCheck(now, activeTermId, termOffset);
            senderPositionLimit = flowControl.onIdle(now);
        }

        retransmitHandler.processTimeouts(now, this);

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

    void senderPositionLimit(final long positionLimit)
    {
        senderPositionLimit = positionLimit;

        if (!hasStatusMessageBeenReceived)
        {
            hasStatusMessageBeenReceived = true;
        }
    }

    /**
     * This is performed on the {@link DriverConductor} thread
     *
     * @return amount of work done
     */
    int cleanLogBuffer()
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

                if (available != channelEndpoint.send(sendBuffer))
                {
                    shortSends.increment();
                    break;
                }

                bytesSent = available + padding(scanOutcome);
                remainingBytes -= bytesSent;
            }
            while (remainingBytes > 0);

            retransmitsSent.orderedIncrement();
        }
    }

    public void triggerSendSetupFrame()
    {
        shouldSendSetupFrame = true;
    }

    RawLog rawLog()
    {
        return rawLog;
    }

    int publisherLimitId()
    {
        return publisherLimit.id();
    }

    /**
     * Update the publishers limit for flow control as part of the conductor duty cycle.
     *
     * @return 1 if the limit has been updated otherwise 0.
     */
    int updatePublishersLimit()
    {
        int workCount = 0;

        final long candidatePublisherLimit =
            hasStatusMessageBeenReceived ? senderPosition.getVolatile() + termWindowLength : 0L;

        if (publisherLimit.proposeMaxOrdered(candidatePublisherLimit))
        {
            workCount = 1;
        }

        return workCount;
    }

    public void onNak(final int termId, final int termOffset, final int length)
    {
        retransmitHandler.onNak(termId, termOffset, length, this);
    }

    public void onStatusMessage(
        final int termId, final int termOffset, final int receiverWindowLength, final InetSocketAddress srcAddress)
    {
        final long position = flowControl.onStatusMessage(termId, termOffset, receiverWindowLength, srcAddress);
        senderPositionLimit(position);

        final long now = epochClock.time();
        LogBufferDescriptor.timeOfLastStatusMessage(rawLog.logMetaData(), now);
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

                if (available == channelEndpoint.send(sendBuffer))
                {
                    timeOfLastSendOrHeartbeat = now;
                    trackSenderLimits = true;

                    bytesSent = available;
                    this.senderPosition.setOrdered(senderPosition + bytesSent + padding(scanOutcome));
                }
                else
                {
                    shortSends.increment();
                }
            }
        }
        else if (trackSenderLimits)
        {
            trackSenderLimits = false;
            senderFlowControlLimits.orderedIncrement();
        }

        return bytesSent;
    }

    private void setupMessageCheck(final long now, final int activeTermId, final int termOffset)
    {
        if (now > (timeOfLastSetup + PUBLICATION_SETUP_TIMEOUT_NS))
        {
            setupBuffer.clear();
            setupHeader
                .activeTermId(activeTermId)
                .termOffset(termOffset)
                .sessionId(sessionId)
                .streamId(streamId)
                .initialTermId(initialTermId)
                .termLength(termLengthMask + 1)
                .mtuLength(mtuLength)
                .ttl(channelEndpoint.multicastTtl());

            final int bytesSent = channelEndpoint.send(setupBuffer);
            if (SetupFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }

            timeOfLastSetup = now;
            timeOfLastSendOrHeartbeat = now;

            if (hasStatusMessageBeenReceived)
            {
                shouldSendSetupFrame = false;
            }
        }
    }

    private void heartbeatMessageCheck(final long now, final int activeTermId, final int termOffset)
    {
        if (now > (timeOfLastSendOrHeartbeat + PUBLICATION_HEARTBEAT_TIMEOUT_NS))
        {
            heartbeatBuffer.clear();
            dataHeader
                .sessionId(sessionId)
                .streamId(streamId)
                .termId(activeTermId)
                .termOffset(termOffset);

            final int bytesSent = channelEndpoint.send(heartbeatBuffer);
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }

            heartbeatsSent.orderedIncrement();
            timeOfLastSendOrHeartbeat = now;
        }
    }

    private boolean isUnreferencedAndPotentiallyInactive(final long now)
    {
        boolean result = false;

        if (0 == refCount)
        {
            final long senderPosition = this.senderPosition.getVolatile();

            timeOfLastActivity = senderPosition == lastSenderPosition ? timeOfLastActivity : now;
            lastSenderPosition = senderPosition;
            result = true;
        }
        else
        {
            timeOfLastActivity = now;
        }

        return result;
    }

    public void onTimeEvent(final long time, final DriverConductor conductor)
    {
        if (isUnreferencedAndPotentiallyInactive(time) && time > (timeOfLastActivity + Configuration.PUBLICATION_LINGER_NS))
        {
            reachedEndOfLife = true;
            conductor.cleanupPublication(NetworkPublication.this);
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    public void timeOfLastStateChange(final long time)
    {
    }

    public long timeOfLastStateChange()
    {
        return timeOfLastActivity;
    }

    public void delete()
    {
        // close is done once sender thread has removed
    }

    public int decRef()
    {
        final int count = --refCount;

        if (0 == count)
        {
            channelEndpoint.removePublication(this);
        }

        return count;
    }

    public int incRef()
    {
        return ++refCount;
    }

    public long producerPosition()
    {
        final UnsafeBuffer logMetaDataBuffer = rawLog.logMetaData();
        final long rawTail = logPartitions[activePartitionIndex(logMetaDataBuffer)].rawTailVolatile();
        final int termOffset = termOffset(rawTail, rawLog.termLength());

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    public long consumerPosition()
    {
        return senderPosition.getVolatile();
    }

    public boolean unblockAtConsumerPosition()
    {
        return LogBufferUnblocker.unblock(logPartitions, rawLog.logMetaData(), consumerPosition());
    }
}
