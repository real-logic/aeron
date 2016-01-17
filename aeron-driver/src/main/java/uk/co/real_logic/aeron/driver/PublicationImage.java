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

import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint;
import uk.co.real_logic.aeron.logbuffer.TermRebuilder;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.UnsafeAccess;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;
import uk.co.real_logic.agrona.concurrent.status.ReadablePosition;

import java.net.InetSocketAddress;
import java.util.List;

import static uk.co.real_logic.aeron.driver.PublicationImage.Status.ACTIVE;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;

class PublicationImagePadding1
{
    @SuppressWarnings("unused")
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

class PublicationImageConductorFields extends PublicationImagePadding1
{
    protected long timeOfLastStatusChange;
    protected long rebuildPosition;

    protected volatile long beginLossChange = -1;
    protected volatile long endLossChange = -1;
    protected int lossTermId;
    protected int lossTermOffset;
    protected int lossLength;
}

class PublicationImagePadding2 extends PublicationImageConductorFields
{
    @SuppressWarnings("unused")
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

class PublicationImageHotFields extends PublicationImagePadding2
{
    protected long lastPacketTimestamp;
    protected long lastStatusMessageTimestamp;
    protected long lastStatusMessagePosition;
    protected long lastChangeNumber = -1;
}

class PublicationImagePadding3 extends PublicationImageHotFields
{
    @SuppressWarnings("unused")
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}

class PublicationImageStatusFields extends PublicationImagePadding3
{
    protected volatile long newStatusMessagePosition;
    protected volatile PublicationImage.Status status = PublicationImage.Status.INIT;
}

class PublicationImagePadding4 extends PublicationImageStatusFields
{
    @SuppressWarnings("unused")
    protected long p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60;
}

/**
 * State maintained for active sessionIds within a channel for receiver processing
 */
public class PublicationImage
    extends PublicationImagePadding4
    implements AutoCloseable, NakMessageSender, DriverManagedResource
{
    public enum Status
    {
        INIT, ACTIVE, INACTIVE, LINGER
    }

    private final long correlationId;
    private final long imageLivenessTimeoutNs;
    private final int sessionId;
    private final int streamId;
    private final int positionBitsToShift;
    private final int termLengthMask;
    private final int initialTermId;
    private final int currentWindowLength;
    private final int currentGain;

    private final RawLog rawLog;
    private final InetSocketAddress controlAddress;
    private final InetSocketAddress sourceAddress;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final SystemCounters systemCounters;
    private final NanoClock clock;
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final Position hwmPosition;
    private final List<ReadablePosition> subscriberPositions;
    private final LossDetector lossDetector;

    private boolean reachedEndOfLife = false;

    public PublicationImage(
        final long correlationId,
        final long imageLivenessTimeoutNs,
        final ReceiveChannelEndpoint channelEndpoint,
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int initialTermOffset,
        final int initialWindowLength,
        final RawLog rawLog,
        final FeedbackDelayGenerator lossFeedbackDelayGenerator,
        final List<ReadablePosition> subscriberPositions,
        final Position hwmPosition,
        final NanoClock clock,
        final SystemCounters systemCounters,
        final InetSocketAddress sourceAddress)
    {
        this.correlationId = correlationId;
        this.imageLivenessTimeoutNs = imageLivenessTimeoutNs;
        this.channelEndpoint = channelEndpoint;
        this.controlAddress = controlAddress;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.rawLog = rawLog;
        this.subscriberPositions = subscriberPositions;
        this.hwmPosition = hwmPosition;
        this.systemCounters = systemCounters;
        this.sourceAddress = sourceAddress;

        this.clock = clock;
        final long time = clock.nanoTime();
        this.timeOfLastStatusChange = time;
        this.lastPacketTimestamp = time;

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = rawLog.partitions()[i].termBuffer();
        }

        this.lossDetector = new LossDetector(lossFeedbackDelayGenerator, this);

        final int termLength = rawLog.termLength();

        this.currentWindowLength = Math.min(termLength, initialWindowLength);
        this.currentGain = Math.min(currentWindowLength / 4, termLength / 4);

        this.termLengthMask = termLength - 1;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        this.initialTermId = initialTermId;

        final long initialPosition = computePosition(activeTermId, initialTermOffset, positionBitsToShift, initialTermId);
        this.lastStatusMessagePosition = initialPosition - (currentGain + 1);
        this.newStatusMessagePosition = this.lastStatusMessagePosition;
        this.rebuildPosition = initialPosition;
        this.hwmPosition.setOrdered(initialPosition);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        rawLog.close();
        hwmPosition.close();
        subscriberPositions.forEach(ReadablePosition::close);
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
    public String channelUriString()
    {
        return channelEndpoint.originalUriString();
    }

    /**
     * The address of the source associated with the image.
     *
     * @return source address
     */
    public InetSocketAddress sourceAddress()
    {
        return sourceAddress;
    }

    /**
     * Return the {@link ReceiveChannelEndpoint} that the image is attached to.
     *
     * @return {@link ReceiveChannelEndpoint} that the image is attached to.
     */
    public ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    /**
     * Remove this image from the {@link DataPacketDispatcher} so it will process no further packets from the network.
     * Called from the {@link Receiver} thread.
     */
    public void removeFromDispatcher()
    {
        channelEndpoint.dispatcher().removePublicationImage(this);
    }

    /**
     * Does this image match a given {@link ReceiveChannelEndpoint} and stream id?
     *
     * @param channelEndpoint to match by identity.
     * @param streamId        to match on value.
     * @return true on a match otherwise false.
     */
    public boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        return this.streamId == streamId && this.channelEndpoint == channelEndpoint;
    }

    /**
     * Get the {@link uk.co.real_logic.aeron.driver.buffer.RawLog} the back this image.
     *
     * @return the {@link uk.co.real_logic.aeron.driver.buffer.RawLog} the back this image.
     */
    public RawLog rawLog()
    {
        return rawLog;
    }

    /**
     * Return status of the image. Retrieved by {@link DriverConductor}.
     *
     * @return status of the image
     */
    public Status status()
    {
        return status;
    }

    /**
     * Set status of the image.
     * <p>
     * Set by {@link Receiver} for INIT to ACTIVE to INACTIVE
     * <p>
     * Set by {@link DriverConductor} for INACTIVE to LINGER
     *
     * @param status of the image
     */
    public void status(final Status status)
    {
        timeOfLastStatusChange = clock.nanoTime();
        this.status = status;
    }

    /**
     * Set status to INACTIVE, but only if currently ACTIVE. Set by {@link Receiver}.
     */
    public void ifActiveGoInactive()
    {
        if (Status.ACTIVE == status)
        {
            status(Status.INACTIVE);
        }
    }

    /**
     * Called from the {@link LossDetector} when gap is detected.
     *
     * @see NakMessageSender
     */
    public void onLossDetected(final int termId, final int termOffset, final int length)
    {
        final long changeNumber = beginLossChange + 1;

        beginLossChange = changeNumber;

        lossTermId = termId;
        lossTermOffset = termOffset;
        lossLength = length;

        endLossChange = changeNumber;
    }

    /**
     * Called from the {@link DriverConductor}.
     *
     * @param now in nanoseconds
     * @return if work has been done or not
     */
    public int trackRebuild(final long now)
    {
        long minSubscriberPosition = Long.MAX_VALUE;
        long maxSubscriberPosition = Long.MIN_VALUE;

        final List<ReadablePosition> subscriberPositions = this.subscriberPositions;
        for (int i = 0, size = subscriberPositions.size(); i < size; i++)
        {
            final long position = subscriberPositions.get(i).getVolatile();
            minSubscriberPosition = Math.min(minSubscriberPosition, position);
            maxSubscriberPosition = Math.max(maxSubscriberPosition, position);
        }

        final long oldRebuildPosition = this.rebuildPosition;
        final long rebuildPosition = Math.max(oldRebuildPosition, maxSubscriberPosition);

        final int positionBitsToShift = this.positionBitsToShift;
        final int index = indexByPosition(rebuildPosition, positionBitsToShift);

        final int workCount = lossDetector.scan(
            termBuffers[index],
            rebuildPosition,
            hwmPosition.getVolatile(),
            now,
            termLengthMask,
            positionBitsToShift,
            initialTermId);

        final int rebuildTermOffset = (int)rebuildPosition & termLengthMask;
        final long newRebuildPosition = (rebuildPosition - rebuildTermOffset) + lossDetector.rebuildOffset();
        this.rebuildPosition = newRebuildPosition;

        final int newTermCount = (int)(newRebuildPosition >>> positionBitsToShift);
        final int oldTermCount = (int)(oldRebuildPosition >>> positionBitsToShift);
        if (newTermCount > oldTermCount)
        {
            final int oldTermCountIndex = indexByTermCount(oldTermCount);
            final UnsafeBuffer termBuffer = termBuffers[previousPartitionIndex(oldTermCountIndex)];
            termBuffer.setMemory(0, termBuffer.capacity(), (byte)0);
        }

        if (minSubscriberPosition > (newStatusMessagePosition + currentGain))
        {
            newStatusMessagePosition = minSubscriberPosition;
        }

        return workCount;
    }

    /**
     * Insert frame into term buffer.
     *
     * @param termId  for the data packet to insert into the appropriate term.
     * @param termOffset for the start of the packet in the term.
     * @param buffer for the data packet to insert into the appropriate term.
     * @param length of the data packet
     * @return number of bytes applied as a result of this insertion.
     */
    public int insertPacket(final int termId, final int termOffset, final UnsafeBuffer buffer, final int length)
    {
        int bytesReceived = length;
        final int positionBitsToShift = this.positionBitsToShift;
        final long packetPosition = computePosition(termId, termOffset, positionBitsToShift, initialTermId);
        final long proposedPosition = packetPosition + length;
        final long windowPosition = lastStatusMessagePosition;

        if (isHeartbeat(buffer, length))
        {
            hwmCandidate(packetPosition);
            systemCounters.heartbeatsReceived().orderedIncrement();
        }
        else if (isFlowControlUnderRun(windowPosition, packetPosition) || isFlowControlOverRun(windowPosition, proposedPosition))
        {
            bytesReceived = 0;
        }
        else
        {
            final UnsafeBuffer termBuffer = termBuffers[indexByPosition(packetPosition, positionBitsToShift)];
            TermRebuilder.insert(termBuffer, termOffset, buffer, length);

            hwmCandidate(proposedPosition);
        }

        return bytesReceived;
    }

    /**
     * To be called from the {@link Receiver} to see if a image should be garbage collected.
     *
     * @param now current time to check against.
     * @return true if still active otherwise false.
     */
    public boolean checkForActivity(final long now)
    {
        boolean activity = true;

        if (now > (lastPacketTimestamp + imageLivenessTimeoutNs))
        {
            activity = false;
        }

        return activity;
    }

    /**
     * Called from the {@link Receiver} to send any pending Status Messages.
     *
     * @param now                  time in nanoseconds.
     * @param statusMessageTimeout for sending of Status Messages.
     * @return number of work items processed.
     */
    public int sendPendingStatusMessage(final long now, final long statusMessageTimeout)
    {
        int workCount = 0;

        if (ACTIVE == status)
        {
            final long statusMessagePosition = this.newStatusMessagePosition;
            if (statusMessagePosition != lastStatusMessagePosition || now > (lastStatusMessageTimestamp + statusMessageTimeout))
            {
                final int termId = computeTermIdFromPosition(statusMessagePosition, positionBitsToShift, initialTermId);
                final int termOffset = (int)statusMessagePosition & termLengthMask;

                channelEndpoint.sendStatusMessage(
                    controlAddress, sessionId, streamId, termId, termOffset, currentWindowLength, (byte)0);

                lastStatusMessageTimestamp = now;
                lastStatusMessagePosition = statusMessagePosition;
                systemCounters.statusMessagesSent().orderedIncrement();
                workCount = 1;
            }
        }

        return workCount;
    }

    /**
     * Called from the {@link Receiver} to send a pending NAK.
     *
     * @return number of work items processed.
     */
    public int sendPendingNak()
    {
        int workCount = 0;
        final long changeNumber = endLossChange;

        if (changeNumber != lastChangeNumber)
        {
            final int termId = lossTermId;
            final int termOffset = lossTermOffset;
            final int length = lossLength;

            UnsafeAccess.UNSAFE.loadFence(); // LoadLoad required so value loads don't move past version check below.

            if (changeNumber == beginLossChange)
            {
                channelEndpoint.sendNakMessage(controlAddress, sessionId, streamId, termId, termOffset, length);
                lastChangeNumber = changeNumber;
                systemCounters.nakMessagesSent().orderedIncrement();
                workCount = 1;
            }
        }

        return workCount;
    }

    /**
     * Remove a {@link ReadablePosition} for a subscriber that has been removed so it is not tracked for flow control.
     *
     * @param subscriberPosition for the subscriber that has been removed.
     */
    public void removeSubscriber(final ReadablePosition subscriberPosition)
    {
        subscriberPositions.remove(subscriberPosition);
        subscriberPosition.close();
    }

    /**
     * Add a new subscriber to this image so their position can be tracked for flow control.
     *
     * @param subscriberPosition for the subscriber to be added.
     */
    public void addSubscriber(final ReadablePosition subscriberPosition)
    {
        subscriberPositions.add(subscriberPosition);
    }

    /**
     * Return number of subscribers to this image.
     *
     * @return number of subscribers
     */
    public int subscriberCount()
    {
        return subscriberPositions.size();
    }

    /**
     * The position up to which the current stream rebuild is complete for reception.
     *
     * @return the position up to which the current stream rebuild is complete for reception.
     */
    public long rebuildPosition()
    {
        return rebuildPosition;
    }

    public void onTimeEvent(final long time, final DriverConductor conductor)
    {
        switch (status)
        {
            case INACTIVE:
                if (isDrained() || time > (timeOfLastStatusChange + imageLivenessTimeoutNs))
                {
                    status(PublicationImage.Status.LINGER);
                    conductor.imageTransitionToLinger(this);
                }
                break;

            case LINGER:
                if (time > (timeOfLastStatusChange + imageLivenessTimeoutNs))
                {
                    reachedEndOfLife = true;
                    conductor.cleanupImage(this);
                }
                break;
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
        return timeOfLastStatusChange;
    }

    public void delete()
    {
        close();
    }

    private boolean isDrained()
    {
        long minSubscriberPosition = Long.MAX_VALUE;
        final List<ReadablePosition> subscriberPositions = this.subscriberPositions;
        for (int i = 0, size = subscriberPositions.size(); i < size; i++)
        {
            minSubscriberPosition = Math.min(minSubscriberPosition, subscriberPositions.get(i).getVolatile());
        }

        return minSubscriberPosition >= rebuildPosition;
    }

    private boolean isHeartbeat(final UnsafeBuffer buffer, final int length)
    {
        return length == DataHeaderFlyweight.HEADER_LENGTH && buffer.getInt(0) == 0;
    }

    private void hwmCandidate(final long proposedPosition)
    {
        lastPacketTimestamp = clock.nanoTime();
        hwmPosition.proposeMaxOrdered(proposedPosition);
    }

    private boolean isFlowControlUnderRun(final long windowPosition, final long packetPosition)
    {
        final boolean isFlowControlUnderRun = packetPosition < windowPosition;

        if (isFlowControlUnderRun)
        {
            systemCounters.flowControlUnderRuns().orderedIncrement();
        }

        return isFlowControlUnderRun;
    }

    private boolean isFlowControlOverRun(final long windowPosition, final long proposedPosition)
    {
        final boolean isFlowControlOverRun = proposedPosition > (windowPosition + currentWindowLength);

        if (isFlowControlOverRun)
        {
            systemCounters.flowControlOverRuns().orderedIncrement();
        }

        return isFlowControlOverRun;
    }
}
