/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.ReadablePosition;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.driver.Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS;
import static io.aeron.driver.Configuration.PUBLICATION_SETUP_TIMEOUT_NS;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.logbuffer.TermScanner.*;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_AND_END_FLAGS;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_END_AND_EOS_FLAGS;

class NetworkPublicationPadding1
{
    @SuppressWarnings("unused")
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

class NetworkPublicationConductorFields extends NetworkPublicationPadding1
{
    protected static final ReadablePosition[] EMPTY_POSITIONS = new ReadablePosition[0];

    protected long cleanPosition = 0;
    protected long timeOfLastActivityNs = 0;
    protected long lastSenderPosition = 0;
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
    protected long timeOfLastSendOrHeartbeatNs;
    protected long timeOfLastSetupNs;
    protected long statusMessageDeadlineNs;
    protected boolean trackSenderLimits = true;
    protected boolean shouldSendSetupFrame = true;
}

class NetworkPublicationPadding3 extends NetworkPublicationSenderFields
{
    @SuppressWarnings("unused")
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}

/**
 * Publication to be sent to connected subscribers.
 */
public class NetworkPublication
    extends NetworkPublicationPadding3
    implements RetransmitSender, DriverManagedResource, Subscribable
{
    public enum State
    {
        ACTIVE, DRAINING, LINGER, CLOSING
    }

    private final long registrationId;
    private final long unblockTimeoutNs;
    private final long connectionTimeoutNs;
    private final long lingerTimeoutNs;
    private final long tag;
    private final int positionBitsToShift;
    private final int initialTermId;
    private final int termBufferLength;
    private final int termLengthMask;
    private final int mtuLength;
    private final int termWindowLength;
    private final int sessionId;
    private final int streamId;
    private final boolean isExclusive;
    private final boolean spiesSimulateConnection;
    private volatile boolean hasReceivers;
    private volatile boolean hasSpies;
    private volatile boolean isConnected;
    private volatile boolean isEndOfStream;
    private volatile boolean hasSenderReleased;
    private State state = State.ACTIVE;

    private final UnsafeBuffer[] termBuffers;
    private final ByteBuffer[] sendBuffers;
    private final Position publisherPos;
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
    private final RetransmitHandler retransmitHandler;
    private final UnsafeBuffer metaDataBuffer;
    private final RawLog rawLog;
    private final AtomicCounter heartbeatsSent;
    private final AtomicCounter retransmitsSent;
    private final AtomicCounter senderFlowControlLimits;
    private final AtomicCounter shortSends;
    private final AtomicCounter unblockedPublications;

    public NetworkPublication(
        final long registrationId,
        final long tag,
        final SendChannelEndpoint channelEndpoint,
        final NanoClock nanoClock,
        final RawLog rawLog,
        final Position publisherPos,
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
        final long connectionTimeoutNs,
        final long lingerTimeoutNs,
        final boolean isExclusive,
        final boolean spiesSimulateConnection)
    {
        this.registrationId = registrationId;
        this.unblockTimeoutNs = unblockTimeoutNs;
        this.connectionTimeoutNs = connectionTimeoutNs;
        this.lingerTimeoutNs = lingerTimeoutNs;
        this.tag = tag;
        this.channelEndpoint = channelEndpoint;
        this.rawLog = rawLog;
        this.nanoClock = nanoClock;
        this.senderPosition = senderPosition;
        this.senderLimit = senderLimit;
        this.flowControl = flowControl;
        this.retransmitHandler = retransmitHandler;
        this.publisherPos = publisherPos;
        this.publisherLimit = publisherLimit;
        this.mtuLength = mtuLength;
        this.initialTermId = initialTermId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.isExclusive = isExclusive;
        this.spiesSimulateConnection = spiesSimulateConnection;

        metaDataBuffer = rawLog.metaData();
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
        termBufferLength = termLength;
        termLengthMask = termLength - 1;
        flowControl.initialize(initialTermId, termLength);

        final long nowNs = nanoClock.nanoTime();
        timeOfLastSendOrHeartbeatNs = nowNs - PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1;
        timeOfLastSetupNs = nowNs - PUBLICATION_SETUP_TIMEOUT_NS - 1;
        statusMessageDeadlineNs = spiesSimulateConnection ? nowNs : (nowNs + connectionTimeoutNs);

        positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        termWindowLength = Configuration.publicationTermWindowLength(termLength);

        lastSenderPosition = senderPosition.get();
        cleanPosition = lastSenderPosition;
        timeOfLastActivityNs = nowNs;
    }

    public void close()
    {
        publisherPos.close();
        publisherLimit.close();
        senderPosition.close();
        senderLimit.close();
        for (final ReadablePosition position : spyPositions)
        {
            position.close();
        }

        rawLog.close();
    }

    public long tag()
    {
        return tag;
    }

    public int termBufferLength()
    {
        return termBufferLength;
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

    public final int send(final long nowNs)
    {
        final long senderPosition = this.senderPosition.get();
        final int activeTermId = computeTermIdFromPosition(senderPosition, positionBitsToShift, initialTermId);
        final int termOffset = (int)senderPosition & termLengthMask;

        if (shouldSendSetupFrame)
        {
            setupMessageCheck(nowNs, activeTermId, termOffset);
        }

        int bytesSent = sendData(nowNs, senderPosition, termOffset);

        if (0 == bytesSent)
        {
            final boolean isEndOfStream = this.isEndOfStream;
            bytesSent = heartbeatMessageCheck(nowNs, activeTermId, termOffset, isEndOfStream);

            if (spiesSimulateConnection && nowNs > statusMessageDeadlineNs && hasSpies)
            {
                final long newSenderPosition = maxSpyPosition(senderPosition);
                this.senderPosition.setOrdered(newSenderPosition);
                senderLimit.setOrdered(flowControl.onIdle(nowNs, newSenderPosition, newSenderPosition, isEndOfStream));
            }
            else
            {
                senderLimit.setOrdered(flowControl.onIdle(nowNs, senderLimit.get(), senderPosition, isEndOfStream));
            }
        }

        updateHasReceivers(nowNs);
        retransmitHandler.processTimeouts(nowNs, this);

        return bytesSent;
    }

    public SendChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    public String channel()
    {
        return channelEndpoint.originalUriString();
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

        if (resendPosition < senderPosition && resendPosition >= (senderPosition - termBufferLength))
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

                final long scanOutcome = scanForAvailability(termBuffer, offset, Math.min(mtuLength, remainingBytes));
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

            retransmitsSent.incrementOrdered();
        }
    }

    public void triggerSendSetupFrame()
    {
        if (!isEndOfStream)
        {
            shouldSendSetupFrame = true;
        }
    }

    public void addSubscriber(final ReadablePosition spyPosition)
    {
        spyPositions = ArrayUtil.add(spyPositions, spyPosition);
        hasSpies = true;

        if (spiesSimulateConnection)
        {
            LogBufferDescriptor.isConnected(metaDataBuffer, true);
            isConnected = true;
        }
    }

    public void removeSubscriber(final ReadablePosition spyPosition)
    {
        spyPositions = ArrayUtil.remove(spyPositions, spyPosition);
        hasSpies = spyPositions.length > 0;
        spyPosition.close();
    }

    public void onNak(final int termId, final int termOffset, final int length)
    {
        retransmitHandler.onNak(termId, termOffset, length, termBufferLength, this);
    }

    public void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress srcAddress)
    {
        if (!hasReceivers)
        {
            hasReceivers = true;
        }

        final long timeNs = nanoClock.nanoTime();
        statusMessageDeadlineNs = timeNs + connectionTimeoutNs;

        final long limit = flowControl.onStatusMessage(
            msg,
            srcAddress,
            senderLimit.get(),
            initialTermId,
            positionBitsToShift,
            timeNs);

        senderLimit.setOrdered(limit);

        if (!isConnected)
        {
            LogBufferDescriptor.isConnected(metaDataBuffer, true);
            isConnected = true;
        }
    }

    public void onRttMeasurement(
        final RttMeasurementFlyweight msg, @SuppressWarnings("unused") final InetSocketAddress srcAddress)
    {
        if (RttMeasurementFlyweight.REPLY_FLAG == (msg.flags() & RttMeasurementFlyweight.REPLY_FLAG))
        {
            // TODO: rate limit

            rttMeasurementHeader
                .receiverId(msg.receiverId())
                .echoTimestampNs(msg.echoTimestampNs())
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
    final int updatePublisherLimit()
    {
        int workCount = 0;

        final long senderPosition = this.senderPosition.getVolatile();
        if (hasReceivers || (spiesSimulateConnection && spyPositions.length > 0))
        {
            long minConsumerPosition = senderPosition;
            for (final ReadablePosition spyPosition : spyPositions)
            {
                minConsumerPosition = Math.min(minConsumerPosition, spyPosition.getVolatile());
            }

            final long proposedPublisherLimit = minConsumerPosition + termWindowLength;
            if (publisherLimit.proposeMaxOrdered(proposedPublisherLimit))
            {
                cleanBuffer(proposedPublisherLimit);
                workCount = 1;
            }
        }
        else if (publisherLimit.get() > senderPosition)
        {
            publisherLimit.setOrdered(senderPosition);
        }

        return workCount;
    }

    boolean hasSpies()
    {
        return hasSpies;
    }

    final void updateHasReceivers(final long timeNs)
    {
        if (timeNs > statusMessageDeadlineNs && hasReceivers)
        {
            hasReceivers = false;
        }
    }

    private int sendData(final long nowNs, final long senderPosition, final int termOffset)
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
                    timeOfLastSendOrHeartbeatNs = nowNs;
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
            senderFlowControlLimits.incrementOrdered();
        }

        return bytesSent;
    }

    private void setupMessageCheck(final long nowNs, final int activeTermId, final int termOffset)
    {
        if (nowNs > (timeOfLastSetupNs + PUBLICATION_SETUP_TIMEOUT_NS))
        {
            timeOfLastSetupNs = nowNs;
            timeOfLastSendOrHeartbeatNs = nowNs;

            setupBuffer.clear();
            setupHeader
                .activeTermId(activeTermId)
                .termOffset(termOffset)
                .sessionId(sessionId)
                .streamId(streamId)
                .initialTermId(initialTermId)
                .termLength(termBufferLength)
                .mtuLength(mtuLength)
                .ttl(channelEndpoint.multicastTtl());

            if (SetupFlyweight.HEADER_LENGTH != channelEndpoint.send(setupBuffer))
            {
                shortSends.increment();
            }

            if (hasReceivers)
            {
                shouldSendSetupFrame = false;
            }
        }
    }

    private int heartbeatMessageCheck(
        final long nowNs, final int activeTermId, final int termOffset, final boolean isEndOfStream)
    {
        int bytesSent = 0;

        if (nowNs > (timeOfLastSendOrHeartbeatNs + PUBLICATION_HEARTBEAT_TIMEOUT_NS))
        {
            heartbeatBuffer.clear();
            heartbeatDataHeader
                .sessionId(sessionId)
                .streamId(streamId)
                .termId(activeTermId)
                .termOffset(termOffset)
                .flags((byte)(isEndOfStream ? BEGIN_END_AND_EOS_FLAGS : BEGIN_AND_END_FLAGS));

            bytesSent = channelEndpoint.send(heartbeatBuffer);
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }

            timeOfLastSendOrHeartbeatNs = nowNs;
            heartbeatsSent.incrementOrdered();
        }

        return bytesSent;
    }

    private void cleanBuffer(final long publisherLimit)
    {
        final long cleanPosition = this.cleanPosition;
        final long dirtyRange = publisherLimit - cleanPosition;
        final int bufferCapacity = termBufferLength;
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

    private void checkForBlockedPublisher(final long producerPosition, final long senderPosition, final long timeNs)
    {
        if (senderPosition == lastSenderPosition && isPossiblyBlocked(producerPosition, senderPosition))
        {
            if (timeNs > (timeOfLastActivityNs + unblockTimeoutNs))
            {
                if (LogBufferUnblocker.unblock(termBuffers, metaDataBuffer, senderPosition, termBufferLength))
                {
                    unblockedPublications.incrementOrdered();
                }
            }
        }
        else
        {
            timeOfLastActivityNs = timeNs;
            lastSenderPosition = senderPosition;
        }
    }

    private boolean isPossiblyBlocked(final long producerPosition, final long consumerPosition)
    {
        final int producerTermCount = activeTermCount(metaDataBuffer);
        final int expectedTermCount = (int)(consumerPosition >> positionBitsToShift);

        if (producerTermCount != expectedTermCount)
        {
            return true;
        }

        return producerPosition > consumerPosition;
    }

    private boolean spiesFinishedConsuming(final DriverConductor conductor, final long eosPosition)
    {
        if (spyPositions.length > 0)
        {
            for (final ReadablePosition spyPosition : spyPositions)
            {
                if (spyPosition.getVolatile() < eosPosition)
                {
                    return false;
                }
            }

            hasSpies = false;
            conductor.cleanupSpies(this);
        }

        return true;
    }

    private long maxSpyPosition(final long senderPosition)
    {
        long position = senderPosition;

        for (final ReadablePosition spyPosition : spyPositions)
        {
            position = Math.max(position, spyPosition.getVolatile());
        }

        return position;
    }

    private void updateConnectedStatus()
    {
        final boolean currentConnectedState = hasReceivers || (spiesSimulateConnection && spyPositions.length > 0);

        if (currentConnectedState != isConnected)
        {
            LogBufferDescriptor.isConnected(metaDataBuffer, currentConnectedState);
            isConnected = currentConnectedState;
        }
    }

    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        updateConnectedStatus();
        final long producerPosition = producerPosition();
        publisherPos.setOrdered(producerPosition);

        switch (state)
        {
            case ACTIVE:
                if (!isExclusive)
                {
                    checkForBlockedPublisher(producerPosition, senderPosition.getVolatile(), timeNs);
                }
                break;

            case DRAINING:
                final long senderPosition = this.senderPosition.getVolatile();
                if (producerPosition > senderPosition)
                {
                    if (LogBufferUnblocker.unblock(termBuffers, metaDataBuffer, senderPosition, termBufferLength))
                    {
                        unblockedPublications.incrementOrdered();
                        break;
                    }

                    if (hasReceivers)
                    {
                        break;
                    }
                }
                else
                {
                    isEndOfStream = true;
                }

                if (spiesFinishedConsuming(conductor, producerPosition))
                {
                    timeOfLastActivityNs = timeNs;
                    state = State.LINGER;
                }
                break;

            case LINGER:
                if (timeNs > (timeOfLastActivityNs + lingerTimeoutNs))
                {
                    conductor.cleanupPublication(this);
                    state = State.CLOSING;
                }
                break;
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return hasSenderReleased;
    }

    public void decRef()
    {
        if (0 == --refCount)
        {
            state = State.DRAINING;
            channelEndpoint.decRef();
            timeOfLastActivityNs = nanoClock.nanoTime();

            final long producerPosition = producerPosition();
            endOfStreamPosition(metaDataBuffer, producerPosition);
            if (senderPosition.getVolatile() >= producerPosition)
            {
                isEndOfStream = true;
            }
        }
    }

    public void incRef()
    {
        ++refCount;
    }

    final State state()
    {
        return state;
    }

    void senderRelease()
    {
        hasSenderReleased = true;
    }

    long producerPosition()
    {
        final long rawTail = rawTailVolatile(metaDataBuffer);
        final int termOffset = termOffset(rawTail, termBufferLength);

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    long consumerPosition()
    {
        return senderPosition.getVolatile();
    }
}
