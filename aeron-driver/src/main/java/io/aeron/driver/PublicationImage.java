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

import io.aeron.Aeron;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.DestinationImageControlAddress;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationUdpTransport;
import io.aeron.driver.reports.LossReport;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.TermRebuilder;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.ReadablePosition;

import java.net.InetSocketAddress;

import static io.aeron.driver.LossDetector.lossFound;
import static io.aeron.driver.LossDetector.rebuildOffset;
import static io.aeron.driver.PublicationImage.State.ACTIVE;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.logbuffer.TermGapFiller.tryFillGap;
import static org.agrona.UnsafeAccess.UNSAFE;

class PublicationImagePadding1
{
    @SuppressWarnings("unused")
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

class PublicationImageConductorFields extends PublicationImagePadding1
{
    protected long cleanPosition;
    protected ReadablePosition[] subscriberPositions;
    protected LossReport lossReport;
    protected LossReport.ReportEntry reportEntry;
}

class PublicationImagePadding2 extends PublicationImageConductorFields
{
    @SuppressWarnings("unused")
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

class PublicationImageReceiverFields extends PublicationImagePadding2
{
    protected boolean isEndOfStream = false;
    protected long lastPacketTimestampNs;
    protected DestinationImageControlAddress[] controlAddresses = new DestinationImageControlAddress[1];
}

class PublicationImagePadding3 extends PublicationImageReceiverFields
{
    @SuppressWarnings("unused")
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}

/**
 * State maintained for active sessionIds within a channel for receiver processing
 */
public class PublicationImage
    extends PublicationImagePadding3
    implements LossHandler, DriverManagedResource, Subscribable
{
    enum State
    {
        INIT, ACTIVE, INACTIVE, LINGER, DONE
    }

    private long timeOfLastStateChangeNs;
    private long lastLossChangeNumber = Aeron.NULL_VALUE;
    private long lastSmChangeNumber = Aeron.NULL_VALUE;

    private volatile long beginLossChange = Aeron.NULL_VALUE;
    private volatile long endLossChange = Aeron.NULL_VALUE;
    private int lossTermId;
    private int lossTermOffset;
    private int lossLength;

    private volatile long beginSmChange = Aeron.NULL_VALUE;
    private volatile long endSmChange = Aeron.NULL_VALUE;
    private long nextSmPosition;
    private int nextSmReceiverWindowLength;

    private long timeOfLastStatusMessageNs;

    private final long correlationId;
    private final long imageLivenessTimeoutNs;
    private final int sessionId;
    private final int streamId;
    private final int positionBitsToShift;
    private final int termLengthMask;
    private final int initialTermId;
    private final boolean isReliable;

    private boolean isTrackingRebuild = true;
    private volatile State state = State.INIT;

    private final NanoClock nanoClock;
    private final NanoClock cachedNanoClock;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final UnsafeBuffer[] termBuffers;
    private final Position hwmPosition;
    private final LossDetector lossDetector;
    private final CongestionControl congestionControl;
    private final Position rebuildPosition;
    private final InetSocketAddress sourceAddress;
    private final AtomicCounter heartbeatsReceived;
    private final AtomicCounter statusMessagesSent;
    private final AtomicCounter nakMessagesSent;
    private final AtomicCounter flowControlUnderRuns;
    private final AtomicCounter flowControlOverRuns;
    private final AtomicCounter lossGapFills;
    private final EpochClock cachedEpochClock;
    private final RawLog rawLog;

    public PublicationImage(
        final long correlationId,
        final long imageLivenessTimeoutNs,
        final ReceiveChannelEndpoint channelEndpoint,
        final int transportIndex,
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int initialTermOffset,
        final RawLog rawLog,
        final FeedbackDelayGenerator lossFeedbackDelayGenerator,
        final ReadablePosition[] subscriberPositions,
        final Position hwmPosition,
        final Position rebuildPosition,
        final NanoClock nanoClock,
        final NanoClock cachedNanoClock,
        final EpochClock cachedEpochClock,
        final SystemCounters systemCounters,
        final InetSocketAddress sourceAddress,
        final CongestionControl congestionControl,
        final LossReport lossReport,
        final boolean isReliable)
    {
        this.correlationId = correlationId;
        this.imageLivenessTimeoutNs = imageLivenessTimeoutNs;
        this.channelEndpoint = channelEndpoint;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.rawLog = rawLog;
        this.subscriberPositions = subscriberPositions;
        this.hwmPosition = hwmPosition;
        this.rebuildPosition = rebuildPosition;
        this.sourceAddress = sourceAddress;
        this.initialTermId = initialTermId;
        this.congestionControl = congestionControl;
        this.lossReport = lossReport;
        this.isReliable = isReliable;

        heartbeatsReceived = systemCounters.get(HEARTBEATS_RECEIVED);
        statusMessagesSent = systemCounters.get(STATUS_MESSAGES_SENT);
        nakMessagesSent = systemCounters.get(NAK_MESSAGES_SENT);
        flowControlUnderRuns = systemCounters.get(FLOW_CONTROL_UNDER_RUNS);
        flowControlOverRuns = systemCounters.get(FLOW_CONTROL_OVER_RUNS);
        lossGapFills = systemCounters.get(LOSS_GAP_FILLS);

        this.nanoClock = nanoClock;
        this.cachedNanoClock = cachedNanoClock;
        this.cachedEpochClock = cachedEpochClock;
        final long nowNs = cachedNanoClock.nanoTime();
        timeOfLastStateChangeNs = nowNs;
        lastPacketTimestampNs = nowNs;

        controlAddresses = ArrayUtil.ensureCapacity(controlAddresses, transportIndex + 1);
        controlAddresses[transportIndex] = new DestinationImageControlAddress(nowNs, controlAddress);

        termBuffers = rawLog.termBuffers();
        lossDetector = new LossDetector(lossFeedbackDelayGenerator, this);

        final int termLength = rawLog.termLength();
        termLengthMask = termLength - 1;
        positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);

        final long initialPosition = computePosition(
            activeTermId, initialTermOffset, positionBitsToShift, initialTermId);
        nextSmPosition = initialPosition;
        nextSmReceiverWindowLength = congestionControl.initialWindowLength();
        cleanPosition = initialPosition;

        hwmPosition.setOrdered(initialPosition);
        rebuildPosition.setOrdered(initialPosition);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        hwmPosition.close();
        rebuildPosition.close();
        for (final ReadablePosition position : subscriberPositions)
        {
            position.close();
        }

        congestionControl.close();
        rawLog.close();
    }

    public long correlationId()
    {
        return correlationId;
    }

    /**
     * The session id of the channel from a publisher.
     *
     * @return session id of the channel from a publisher.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * The stream id of this image within a channel.
     *
     * @return stream id of this image within a channel.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Get the string representation of the channel URI.
     *
     * @return the string representation of the channel URI.
     */
    public String channel()
    {
        return channelEndpoint.originalUriString();
    }

    /**
     * Remove a {@link ReadablePosition} for a subscriber that has been removed so it is not tracked for flow control.
     *
     * @param subscriberPosition for the subscriber that has been removed.
     */
    public void removeSubscriber(final ReadablePosition subscriberPosition)
    {
        subscriberPositions = ArrayUtil.remove(subscriberPositions, subscriberPosition);
        subscriberPosition.close();
    }

    /**
     * Add a new subscriber to this image so their position can be tracked for flow control.
     *
     * @param subscriberPosition for the subscriber to be added.
     */
    public void addSubscriber(final ReadablePosition subscriberPosition)
    {
        subscriberPositions = ArrayUtil.add(subscriberPositions, subscriberPosition);
    }

    /**
     * Called from the {@link LossDetector} when gap is detected by the {@link DriverConductor} thread.
     *
     * @see LossHandler
     */
    public void onGapDetected(final int termId, final int termOffset, final int length)
    {
        final long changeNumber = beginLossChange + 1;

        beginLossChange = changeNumber;

        lossTermId = termId;
        lossTermOffset = termOffset;
        lossLength = length;

        endLossChange = changeNumber;

        if (null != reportEntry)
        {
            reportEntry.recordObservation(length, cachedEpochClock.time());
        }
        else if (null != lossReport)
        {
            reportEntry = lossReport.createEntry(
                length, cachedEpochClock.time(), sessionId, streamId, channel(), sourceAddress.toString());

            if (null == reportEntry)
            {
                lossReport = null;
            }
        }
    }

    /**
     * The address of the source associated with the image.
     *
     * @return source address
     */
    InetSocketAddress sourceAddress()
    {
        return sourceAddress;
    }

    /**
     * Return the {@link ReceiveChannelEndpoint} that the image is attached to.
     *
     * @return {@link ReceiveChannelEndpoint} that the image is attached to.
     */
    ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    /**
     * Remove this image from the {@link DataPacketDispatcher} so it will process no further packets from the network.
     * Called from the {@link Receiver} thread.
     */
    void removeFromDispatcher()
    {
        channelEndpoint.removePublicationImage(this);
    }

    /**
     * Get the {@link RawLog} the back this image.
     *
     * @return the {@link RawLog} the back this image.
     */
    RawLog rawLog()
    {
        return rawLog;
    }

    /**
     * Activate this image from the {@link Receiver}
     */
    void activate()
    {
        state(ACTIVE);
    }

    void addDestination(final int transportIndex, final ReceiveDestinationUdpTransport transport)
    {
        controlAddresses = ArrayUtil.ensureCapacity(controlAddresses, transportIndex + 1);

        if (transport.isMulticast())
        {
            controlAddresses[transportIndex] =
                new DestinationImageControlAddress(nanoClock.nanoTime(), transport.udpChannel().remoteControl());
        }
        else if (transport.hasExplicitControl())
        {
            controlAddresses[transportIndex] =
                new DestinationImageControlAddress(nanoClock.nanoTime(), transport.explicitControlAddress());
        }
    }

    void removeDestination(final int transportIndex)
    {
        controlAddresses[transportIndex] = null;
    }

    void addControlAddressIfUnknown(final int transportIndex, final InetSocketAddress remoteAddress)
    {
        updateControlAddress(transportIndex, remoteAddress, nanoClock.nanoTime());
    }

    private void state(final State state)
    {
        timeOfLastStateChangeNs = cachedNanoClock.nanoTime();
        this.state = state;
    }

    private void scheduleStatusMessage(final long nowNs, final long smPosition, final int receiverWindowLength)
    {
        final long changeNumber = beginSmChange + 1;

        beginSmChange = changeNumber;

        nextSmPosition = smPosition;
        nextSmReceiverWindowLength = receiverWindowLength;
        timeOfLastStatusMessageNs = nowNs;

        endSmChange = changeNumber;
    }

    /**
     * Called from the {@link DriverConductor}.
     *
     * @param nowNs                  in nanoseconds
     * @param statusMessageTimeoutNs for sending of Status Messages.
     */
    final void trackRebuild(final long nowNs, final long statusMessageTimeoutNs)
    {
        long minSubscriberPosition = Long.MAX_VALUE;
        long maxSubscriberPosition = Long.MIN_VALUE;

        for (final ReadablePosition subscriberPosition : subscriberPositions)
        {
            final long position = subscriberPosition.getVolatile();
            minSubscriberPosition = Math.min(minSubscriberPosition, position);
            maxSubscriberPosition = Math.max(maxSubscriberPosition, position);
        }

        final long rebuildPosition = Math.max(this.rebuildPosition.get(), maxSubscriberPosition);
        final long hwmPosition = this.hwmPosition.getVolatile();

        final long scanOutcome = lossDetector.scan(
            termBuffers[indexByPosition(rebuildPosition, positionBitsToShift)],
            rebuildPosition,
            hwmPosition,
            nowNs,
            termLengthMask,
            positionBitsToShift,
            initialTermId);

        final int rebuildTermOffset = (int)rebuildPosition & termLengthMask;
        final long newRebuildPosition = (rebuildPosition - rebuildTermOffset) + rebuildOffset(scanOutcome);
        this.rebuildPosition.proposeMaxOrdered(newRebuildPosition);

        final long ccOutcome = congestionControl.onTrackRebuild(
            nowNs,
            minSubscriberPosition,
            nextSmPosition,
            hwmPosition,
            rebuildPosition,
            newRebuildPosition,
            lossFound(scanOutcome));

        final int window = CongestionControlUtil.receiverWindowLength(ccOutcome);
        final long threshold = CongestionControlUtil.positionThreshold(window);

        if (CongestionControlUtil.shouldForceStatusMessage(ccOutcome) ||
            (nowNs > (timeOfLastStatusMessageNs + statusMessageTimeoutNs)) ||
            (minSubscriberPosition > (nextSmPosition + threshold)))
        {
            scheduleStatusMessage(nowNs, minSubscriberPosition, window);
            cleanBufferTo(minSubscriberPosition - (termLengthMask + 1));
        }
    }

    /**
     * Set state to {@link State#INACTIVE} if currently {@link State#ACTIVE}. Set by {@link Receiver}.
     */
    void ifActiveGoInactive()
    {
        if (State.ACTIVE == state)
        {
            state(State.INACTIVE);
        }
    }

    final boolean isTrackingRebuild()
    {
        return isTrackingRebuild;
    }

    /**
     * Insert frame into term buffer.
     *
     * @param termId     for the data packet to insert into the appropriate term.
     * @param termOffset for the start of the packet in the term.
     * @param buffer     for the data packet to insert into the appropriate term.
     * @param length     of the data packet
     * @return number of bytes applied as a result of this insertion.
     */
    int insertPacket(
        final int termId,
        final int termOffset,
        final UnsafeBuffer buffer,
        final int length,
        final int transportIndex,
        final InetSocketAddress srcAddress)
    {
        final boolean isHeartbeat = DataHeaderFlyweight.isHeartbeat(buffer, length);
        final long packetPosition = computePosition(termId, termOffset, positionBitsToShift, initialTermId);
        final long proposedPosition = isHeartbeat ? packetPosition : packetPosition + length;
        final long windowPosition = nextSmPosition;

        if (!isFlowControlUnderRun(windowPosition, packetPosition) &&
            !isFlowControlOverRun(windowPosition, proposedPosition))
        {
            if (isHeartbeat)
            {
                if (!isEndOfStream && DataHeaderFlyweight.isEndOfStream(buffer))
                {
                    isEndOfStream = true;
                    LogBufferDescriptor.endOfStreamPosition(rawLog.metaData(), packetPosition);
                }

                heartbeatsReceived.incrementOrdered();
            }
            else
            {
                final UnsafeBuffer termBuffer = termBuffers[indexByPosition(packetPosition, positionBitsToShift)];
                TermRebuilder.insert(termBuffer, termOffset, buffer, length);
            }

            lastPacketTimestampNs = cachedNanoClock.nanoTime();
            hwmPosition.proposeMaxOrdered(proposedPosition);
            updateControlAddress(transportIndex, srcAddress, lastPacketTimestampNs);
        }

        return length;
    }

    /**
     * To be called from the {@link Receiver} to see if a image should be retained.
     *
     * @param nowNs current time to check against for activity.
     * @return true if the image should be retained otherwise false.
     */
    boolean hasActivityAndNotEndOfStream(final long nowNs)
    {
        boolean isActive = true;

        if (nowNs > (lastPacketTimestampNs + imageLivenessTimeoutNs) ||
            (isEndOfStream && rebuildPosition.getVolatile() >= hwmPosition.get()))
        {
            isActive = false;
        }

        return isActive;
    }

    /**
     * Called from the {@link Receiver} to send any pending Status Messages.
     *
     * @return number of work items processed.
     */
    int sendPendingStatusMessage()
    {
        int workCount = 0;

        if (ACTIVE == state)
        {
            final long changeNumber = endSmChange;

            if (changeNumber != lastSmChangeNumber)
            {
                final long smPosition = nextSmPosition;
                final int receiverWindowLength = nextSmReceiverWindowLength;

                UNSAFE.loadFence(); // LoadLoad required so previous loads don't move past version check below.

                if (changeNumber == beginSmChange)
                {
                    final int termId = computeTermIdFromPosition(smPosition, positionBitsToShift, initialTermId);
                    final int termOffset = (int)smPosition & termLengthMask;

                    channelEndpoint.sendStatusMessage(
                        controlAddresses, sessionId, streamId, termId, termOffset, receiverWindowLength, (byte)0);

                    statusMessagesSent.incrementOrdered();

                    lastSmChangeNumber = changeNumber;
                }

                workCount = 1;
            }
        }

        return workCount;
    }

    /**
     * Called from the {@link Receiver} thread to processing any pending loss of packets.
     *
     * @return number of work items processed.
     */
    int processPendingLoss()
    {
        int workCount = 0;
        final long changeNumber = endLossChange;

        if (changeNumber != lastLossChangeNumber)
        {
            final int termId = lossTermId;
            final int termOffset = lossTermOffset;
            final int length = lossLength;

            UNSAFE.loadFence(); // LoadLoad required so previous loads don't move past version check below.

            if (changeNumber == beginLossChange)
            {
                if (isReliable)
                {
                    channelEndpoint.sendNakMessage(controlAddresses, sessionId, streamId, termId, termOffset, length);
                    nakMessagesSent.incrementOrdered();
                }
                else
                {
                    final UnsafeBuffer termBuffer = termBuffers[indexByTerm(initialTermId, termId)];
                    if (tryFillGap(rawLog.metaData(), termBuffer, termId, termOffset, length))
                    {
                        lossGapFills.incrementOrdered();
                    }
                }

                lastLossChangeNumber = changeNumber;
            }

            workCount = 1;
        }

        return workCount;
    }

    /**
     * Called from the {@link Receiver} thread to check for initiating an RTT measurement.
     *
     * @param nowNs in nanoseconds
     * @return number of work items processed.
     */
    int initiateAnyRttMeasurements(final long nowNs)
    {
        int workCount = 0;

        if (congestionControl.shouldMeasureRtt(nowNs))
        {
            final long preciseTimeNs = nanoClock.nanoTime();

            channelEndpoint.sendRttMeasurement(controlAddresses, sessionId, streamId, preciseTimeNs, 0, true);
            congestionControl.onRttMeasurementSent(preciseTimeNs);

            workCount = 1;
        }

        return workCount;
    }

    /**
     * Called from the {@link Receiver} upon receiving an RTT Measurement that is a reply.
     *
     * @param header         of the measurement
     * @param transportIndex that the RTT Measurement came in on.
     * @param srcAddress     from the sender of the measurement
     */
    void onRttMeasurement(
        final RttMeasurementFlyweight header, final int transportIndex, final InetSocketAddress srcAddress)
    {
        final long nowNs = nanoClock.nanoTime();
        final long rttInNs = nowNs - header.echoTimestampNs() - header.receptionDelta();

        congestionControl.onRttMeasurement(nowNs, rttInNs, srcAddress);
    }

    /**
     * Is the image in a state to accept new subscriptions?
     *
     * @return true if accepting new subscriptions.
     */
    boolean isAcceptingSubscriptions()
    {
        return subscriberPositions.length > 0 && state == ACTIVE;
    }

    /**
     * The position up to which the current stream rebuild is complete for reception.
     *
     * @return the position up to which the current stream rebuild is complete for reception.
     */
    long rebuildPosition()
    {
        return rebuildPosition.get();
    }

    public void onTimeEvent(final long timeNs, final long timesMs, final DriverConductor conductor)
    {
        switch (state)
        {
            case INACTIVE:
                if (isDrained() || timeNs > (timeOfLastStateChangeNs + imageLivenessTimeoutNs))
                {
                    state = State.LINGER;
                    timeOfLastStateChangeNs = timeNs;
                    conductor.transitionToLinger(this);
                }
                isTrackingRebuild = false;
                break;

            case LINGER:
                if (timeNs > (timeOfLastStateChangeNs + imageLivenessTimeoutNs))
                {
                    state = State.DONE;
                    conductor.cleanupImage(this);
                }
                break;
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return State.DONE == state;
    }

    private boolean isDrained()
    {
        final long rebuildPosition = this.rebuildPosition.get();

        for (final ReadablePosition subscriberPosition : subscriberPositions)
        {
            if (subscriberPosition.getVolatile() < rebuildPosition)
            {
                return false;
            }
        }

        return true;
    }

    private boolean isFlowControlUnderRun(final long windowPosition, final long packetPosition)
    {
        final boolean isFlowControlUnderRun = packetPosition < windowPosition;

        if (isFlowControlUnderRun)
        {
            flowControlUnderRuns.incrementOrdered();
        }

        return isFlowControlUnderRun;
    }

    private boolean isFlowControlOverRun(final long windowPosition, final long proposedPosition)
    {
        final boolean isFlowControlOverRun = proposedPosition > (windowPosition + nextSmReceiverWindowLength);

        if (isFlowControlOverRun)
        {
            flowControlOverRuns.incrementOrdered();
        }

        return isFlowControlOverRun;
    }

    private void cleanBufferTo(final long newCleanPosition)
    {
        final long cleanPosition = this.cleanPosition;
        final int bytesForCleaning = (int)(newCleanPosition - cleanPosition);
        final UnsafeBuffer dirtyTerm = termBuffers[indexByPosition(cleanPosition, positionBitsToShift)];
        final int termOffset = (int)cleanPosition & termLengthMask;
        final int length = Math.min(bytesForCleaning, dirtyTerm.capacity() - termOffset);

        if (length > 0)
        {
            dirtyTerm.setMemory(termOffset, length, (byte)0);
            this.cleanPosition = cleanPosition + length;
        }
    }

    private void updateControlAddress(final int transportIndex, final InetSocketAddress srcAddress, final long nowNs)
    {
        DestinationImageControlAddress controlAddress = controlAddresses[transportIndex];

        if (null == controlAddress)
        {
            controlAddress = new DestinationImageControlAddress(nowNs, srcAddress);
            controlAddresses[transportIndex] = controlAddress;
        }

        controlAddress.timeOfLastFrameNs = nowNs;
    }
}
