/*
 * Copyright 2014-2017 Real Logic Ltd.
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
import io.aeron.logbuffer.LogBufferUnblocker;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.ReadablePosition;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.Aeron.PUBLICATION_CONNECTION_TIMEOUT_MS;
import static io.aeron.driver.Configuration.*;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.logbuffer.TermScanner.*;

class NetworkPublicationPadding1
{
    @SuppressWarnings("unused")
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

class NetworkPublicationConductorFields extends NetworkPublicationPadding1
{
    private static final ReadablePosition[] EMPTY_POSITIONS = new ReadablePosition[0];

    protected long cleanPosition = 0;
    protected long timeOfLastActivity = 0;
    protected long lastSenderPosition = 0;
    protected long lastConsumerPosition = 0;
    protected long timeOfLastConsumerPositionChange = 0;
    protected int refCount = 0;
    protected ReadablePosition[] spyPositions = EMPTY_POSITIONS;
}

class NetworkPublicationPadding2 extends NetworkPublicationConductorFields
{
    @SuppressWarnings("unused")
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

class NetworkPublicationSenderFields extends NetworkPublicationPadding2
{
    protected long timeOfLastSendOrHeartbeat;
    protected long timeOfLastSetup;
    protected boolean trackSenderLimits = true;
    protected boolean shouldSendSetupFrame = true;
}

class NetworkPublicationPadding3 extends NetworkPublicationSenderFields
{
    @SuppressWarnings("unused")
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}

/**
 * Publication to be sent to registered subscribers.
 */
public class NetworkPublication
    extends NetworkPublicationPadding3
    implements RetransmitSender, DriverManagedResource, Subscribable
{
    public enum Status
    {
        ACTIVE, INACTIVE
    }

    private final long registrationId;
    private final long unblockTimeoutNs;
    private final int positionBitsToShift;
    private final int initialTermId;
    private final int termLengthMask;
    private final int mtuLength;
    private final int termWindowLength;
    private final int sessionId;
    private final int streamId;
    private final boolean isExclusive;
    private volatile boolean isConnected = false;
    private boolean hasReachedEndOfLife = false;
    private Status status = Status.ACTIVE;

    private final UnsafeBuffer[] termBuffers;
    private final ByteBuffer[] sendBuffers;
    private final Position publisherLimit;
    private final Position senderPosition;
    private final Position senderLimit;
    private final SendChannelEndpoint channelEndpoint;
    private final ByteBuffer heartbeatBuffer;
    private final DataHeaderFlyweight heartbeatDataHeader;
    private final ByteBuffer setupBuffer;
    private final SetupFlyweight setupHeader;
    private final ByteBuffer rttMeasurementBuffer;
    private final RttMeasurementFlyweight rttMeasurementHeader;
    private final FlowControl flowControl;
    private final NanoClock nanoClock;
    private final EpochClock epochClock;
    private final RetransmitHandler retransmitHandler;
    private final RawLog rawLog;
    private final AtomicCounter heartbeatsSent;
    private final AtomicCounter retransmitsSent;
    private final AtomicCounter senderFlowControlLimits;
    private final AtomicCounter shortSends;
    private final AtomicCounter unblockedPublications;

    public NetworkPublication(
        final long registrationId,
        final SendChannelEndpoint channelEndpoint,
        final NanoClock nanoClock,
        final EpochClock epochClock,
        final RawLog rawLog,
        final Position publisherLimit,
        final Position senderPosition,
        final Position senderLimit,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int mtuLength,
        final SystemCounters systemCounters,
        final FlowControl flowControl,
        final RetransmitHandler retransmitHandler,
        final NetworkPublicationThreadLocals threadLocals,
        final long unblockTimeoutNs,
        final boolean isExclusive)
    {
        this.registrationId = registrationId;
        this.unblockTimeoutNs = unblockTimeoutNs;
        this.channelEndpoint = channelEndpoint;
        this.rawLog = rawLog;
        this.nanoClock = nanoClock;
        this.epochClock = epochClock;
        this.senderPosition = senderPosition;
        this.senderLimit = senderLimit;
        this.flowControl = flowControl;
        this.retransmitHandler = retransmitHandler;
        this.publisherLimit = publisherLimit;
        this.mtuLength = mtuLength;
        this.initialTermId = initialTermId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.isExclusive = isExclusive;

        setupBuffer = threadLocals.setupBuffer();
        setupHeader = threadLocals.setupHeader();
        heartbeatBuffer = threadLocals.heartbeatBuffer();
        heartbeatDataHeader = threadLocals.heartbeatDataHeader();
        rttMeasurementBuffer = threadLocals.rttMeasurementBuffer();
        rttMeasurementHeader = threadLocals.rttMeasurementHeader();

        heartbeatsSent = systemCounters.get(HEARTBEATS_SENT);
        shortSends = systemCounters.get(SHORT_SENDS);
        retransmitsSent = systemCounters.get(RETRANSMITS_SENT);
        senderFlowControlLimits = systemCounters.get(SENDER_FLOW_CONTROL_LIMITS);
        unblockedPublications = systemCounters.get(UNBLOCKED_PUBLICATIONS);

        termBuffers = rawLog.termBuffers();
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
        publisherLimit.close();
        senderPosition.close();
        senderLimit.close();
        for (final ReadablePosition position : spyPositions)
        {
            position.close();
        }

        rawLog.close();
    }

    public int mtuLength()
    {
        return mtuLength;
    }

    public long registrationId()
    {
        return registrationId;
    }

    public boolean isExclusive()
    {
        return isExclusive;
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

        int bytesSent = sendData(now, senderPosition, termOffset);

        if (0 == bytesSent)
        {
            bytesSent = heartbeatMessageCheck(now, activeTermId, termOffset);
            senderLimit.setOrdered(flowControl.onIdle(now, senderLimit.get()));
        }

        retransmitHandler.processTimeouts(now, this);

        return bytesSent;
    }

    public SendChannelEndpoint channelEndpoint()
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

    public void resend(final int termId, final int termOffset, final int length)
    {
        final long senderPosition = this.senderPosition.get();
        final long resendPosition = computePosition(termId, termOffset, positionBitsToShift, initialTermId);

        if (resendPosition < senderPosition && resendPosition >= (senderPosition - rawLog.termLength()))
        {
            final int activeIndex = indexByPosition(resendPosition, positionBitsToShift);
            final UnsafeBuffer termBuffer = termBuffers[activeIndex];
            final ByteBuffer sendBuffer = sendBuffers[activeIndex];

            int remainingBytes = length;
            int bytesSent = 0;
            int offset = termOffset;
            do
            {
                offset += bytesSent;

                final long scanOutcome = scanForAvailability(termBuffer, offset, mtuLength);
                final int available = available(scanOutcome);
                if (available <= 0)
                {
                    break;
                }

                sendBuffer.limit(offset + available).position(offset);

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

    public void addSubscriber(final ReadablePosition spyPosition)
    {
        spyPositions = ArrayUtil.add(spyPositions, spyPosition);
    }

    public void removeSubscriber(final ReadablePosition spyPosition)
    {
        spyPositions = ArrayUtil.remove(spyPositions, spyPosition);
        spyPosition.close();
    }

    public void onNak(final int termId, final int termOffset, final int length)
    {
        retransmitHandler.onNak(termId, termOffset, length, termLengthMask + 1, this);
    }

    public void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress srcAddress)
    {
        LogBufferDescriptor.timeOfLastStatusMessage(rawLog.metaData(), epochClock.time());

        if (!isConnected)
        {
            isConnected = true;
        }

        senderLimit.setOrdered(
            flowControl.onStatusMessage(
                msg,
                srcAddress,
                senderLimit.get(),
                initialTermId,
                positionBitsToShift,
                nanoClock.nanoTime()));
    }

    public void onRttMeasurement(final RttMeasurementFlyweight msg, final InetSocketAddress srcAddress)
    {
        if (RttMeasurementFlyweight.REPLY_FLAG == (msg.flags() & RttMeasurementFlyweight.REPLY_FLAG))
        {
            // TODO: rate limit

            rttMeasurementHeader
                .receiverId(msg.receiverId())
                .echoTimestamp(msg.echoTimestamp())
                .receptionDelta(0)
                .sessionId(sessionId)
                .streamId(streamId)
                .flags((short)0x0);

            final int bytesSent = channelEndpoint.send(rttMeasurementBuffer);
            if (RttMeasurementFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }
        }

        // handling of RTT measurements would be done in an else clause here.
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

        if (isConnected)
        {
            long minConsumerPosition = senderPosition.getVolatile();
            if (spyPositions.length > 0)
            {
                for (final ReadablePosition spyPosition : spyPositions)
                {
                    minConsumerPosition = Math.min(minConsumerPosition, spyPosition.getVolatile());
                }
            }

            final long proposedPublisherLimit = minConsumerPosition + termWindowLength;
            if (publisherLimit.proposeMaxOrdered(proposedPublisherLimit))
            {
                cleanBuffer(proposedPublisherLimit);
                workCount = 1;
            }
        }
        else
        {
            publisherLimit.setOrdered(senderPosition.getVolatile());
        }

        return workCount;
    }

    boolean hasSpies()
    {
        return spyPositions.length > 0;
    }

    long spyJoiningPosition()
    {
        long maxSpyPosition = producerPosition();

        for (final ReadablePosition spyPosition : spyPositions)
        {
            maxSpyPosition = Math.max(maxSpyPosition, spyPosition.getVolatile());
        }

        return maxSpyPosition;
    }

    private int sendData(final long now, final long senderPosition, final int termOffset)
    {
        int bytesSent = 0;
        final int availableWindow = (int)(senderLimit.get() - senderPosition);
        if (availableWindow > 0)
        {
            final int scanLimit = Math.min(availableWindow, mtuLength);
            final int activeIndex = indexByPosition(senderPosition, positionBitsToShift);

            final long scanOutcome = scanForAvailability(termBuffers[activeIndex], termOffset, scanLimit);
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

    private void setupMessageCheck(final long nowNs, final int activeTermId, final int termOffset)
    {
        if (nowNs > (timeOfLastSetup + PUBLICATION_SETUP_TIMEOUT_NS))
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

            timeOfLastSetup = nowNs;
            timeOfLastSendOrHeartbeat = nowNs;

            if (isConnected)
            {
                shouldSendSetupFrame = false;
            }
        }
    }

    private int heartbeatMessageCheck(final long nowNs, final int activeTermId, final int termOffset)
    {
        int bytesSent = 0;

        if (nowNs > (timeOfLastSendOrHeartbeat + PUBLICATION_HEARTBEAT_TIMEOUT_NS))
        {
            heartbeatBuffer.clear();
            heartbeatDataHeader
                .sessionId(sessionId)
                .streamId(streamId)
                .termId(activeTermId)
                .termOffset(termOffset);

            bytesSent = channelEndpoint.send(heartbeatBuffer);
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }

            heartbeatsSent.orderedIncrement();
            timeOfLastSendOrHeartbeat = nowNs;
        }

        return bytesSent;
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

    private void cleanBuffer(final long publisherLimit)
    {
        final long cleanPosition = this.cleanPosition;
        final long dirtyRange = publisherLimit - cleanPosition;
        final int bufferCapacity = termLengthMask + 1;
        final int reservedRange = bufferCapacity * 2;

        if (dirtyRange > reservedRange)
        {
            final UnsafeBuffer dirtyTerm = termBuffers[indexByPosition(cleanPosition, positionBitsToShift)];
            final int termOffset = (int)cleanPosition & termLengthMask;
            final int bytesForCleaning = (int)(dirtyRange - reservedRange);
            final int length = Math.min(bytesForCleaning, bufferCapacity - termOffset);

            dirtyTerm.setMemory(termOffset, length, (byte)0);
            this.cleanPosition = cleanPosition + length;
        }
    }

    private void checkForBlockedPublisher(final long timeNs)
    {
        final long consumerPosition = senderPosition.getVolatile();
        if (consumerPosition == lastConsumerPosition)
        {
            if (producerPosition() > consumerPosition &&
                timeNs > (timeOfLastConsumerPositionChange + unblockTimeoutNs))
            {
                if (unblockAtConsumerPosition())
                {
                    unblockedPublications.orderedIncrement();
                }
            }
        }
        else
        {
            timeOfLastConsumerPositionChange = timeNs;
            lastConsumerPosition = consumerPosition;
        }
    }

    private boolean haveSpiesCaughtUpWithTheSender()
    {
        final long senderPosition = this.senderPosition.getVolatile();

        for (final ReadablePosition spyPosition : spyPositions)
        {
            if (spyPosition.getVolatile() < senderPosition)
            {
                return false;
            }
        }

        return true;
    }

    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (isUnreferencedAndPotentiallyInactive(timeNs) &&
            timeNs > (timeOfLastActivity + PUBLICATION_LINGER_NS) &&
            haveSpiesCaughtUpWithTheSender())
        {
            hasReachedEndOfLife = true;
            conductor.cleanupPublication(NetworkPublication.this);
        }
        else
        {
            if (!isExclusive)
            {
                checkForBlockedPublisher(timeNs);
            }

            if (isConnected &&
                timeMs > (timeOfLastStatusMessage(rawLog.metaData()) + PUBLICATION_CONNECTION_TIMEOUT_MS))
            {
                isConnected = false;
            }
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return hasReachedEndOfLife;
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
            status = Status.INACTIVE;
            channelEndpoint.decRef();
        }

        return count;
    }

    public int incRef()
    {
        return ++refCount;
    }

    public Status status()
    {
        return status;
    }

    public long producerPosition()
    {
        final long rawTail = rawTailVolatile(rawLog.metaData());
        final int termOffset = termOffset(rawTail, rawLog.termLength());

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    public long consumerPosition()
    {
        return senderPosition.getVolatile();
    }

    public boolean unblockAtConsumerPosition()
    {
        return LogBufferUnblocker.unblock(termBuffers, rawLog.metaData(), senderPosition.getVolatile());
    }
}
