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
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogRebuilder;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.status.PositionIndicator;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.common.TermHelper.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.IN_CLEANING;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.NEEDS_CLEANING;

/**
 * State maintained for active sessionIds within a channel for receiver processing
 */
public class DriverConnection implements AutoCloseable
{
    public enum Status {ACTIVE, INACTIVE, LINGER}

    private final ReceiveChannelEndpoint channelEndpoint;
    private final long correlationId;
    private final int sessionId;
    private final int streamId;
    private final TermBuffers termBuffers;
    private PositionIndicator[] subscriberPositions;
    private final NanoClock clock;
    private final PositionReporter completedPosition;
    private final PositionReporter hwmPosition;
    private final SystemCounters systemCounters;
    private final EventLogger logger;

    private final AtomicInteger activeTermId = new AtomicInteger();
    private final AtomicLong timeOfLastFrame = new AtomicLong();
    private int activeIndex;
    private int hwmTermId;
    private int hwmIndex;
    private Status status;
    private long timeOfLastStatusChange;

    private final LogRebuilder[] rebuilders;
    private final LossHandler lossHandler;
    private final StatusMessageSender statusMessageSender;

    private final int positionBitsToShift;
    private final int initialTermId;
    private final long statusMessageTimeout;

    private long lastSmPosition;
    private long lastSmTimestamp;
    private int lastSmTermId;
    private int currentWindowSize;
    private int currentGain;

    private volatile boolean statusMessagesEnabled = false;
    private volatile boolean scanForGapsEnabled = true;

    public DriverConnection(
        final ReceiveChannelEndpoint channelEndpoint,
        final long correlationId,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int initialTermOffset,
        final int initialWindowSize,
        final long statusMessageTimeout,
        final TermBuffers termBuffers,
        final LossHandler lossHandler,
        final StatusMessageSender statusMessageSender,
        final PositionIndicator[] positions,
        final PositionReporter completedPosition,
        final PositionReporter hwmPosition,
        final NanoClock clock,
        final SystemCounters systemCounters,
        final EventLogger logger)
    {
        this.channelEndpoint = channelEndpoint;
        this.correlationId = correlationId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.termBuffers = termBuffers;
        this.subscriberPositions = positions;
        this.completedPosition = completedPosition;
        this.hwmPosition = hwmPosition;
        this.systemCounters = systemCounters;
        this.logger = logger;
        this.status = Status.ACTIVE;
        this.timeOfLastStatusChange = clock.time();

        this.clock = clock;
        activeTermId.lazySet(initialTermId);
        timeOfLastFrame.lazySet(clock.time());
        this.hwmIndex = this.activeIndex = termIdToBufferIndex(initialTermId);
        this.hwmTermId = initialTermId;

        rebuilders =
            termBuffers.stream()
                       .map((rawLog) -> new LogRebuilder(rawLog.logBuffer(), rawLog.stateBuffer()))
                       .toArray(LogRebuilder[]::new);
        this.lossHandler = lossHandler;
        this.statusMessageSender = statusMessageSender;
        this.statusMessageTimeout = statusMessageTimeout;
        this.lastSmTermId = initialTermId;
        this.lastSmTimestamp = 0;

        final int termCapacity = rebuilders[0].capacity();

        this.currentWindowSize = Math.min(termCapacity, initialWindowSize);
        this.currentGain = Math.min(currentWindowSize / 4, termCapacity / 4);

        this.positionBitsToShift = Integer.numberOfTrailingZeros(termCapacity);
        this.initialTermId = initialTermId;

        final long initialPosition = TermHelper.calculatePosition(
            initialTermId, initialTermOffset, positionBitsToShift, initialTermId);

        this.lastSmPosition = initialPosition;

        rebuilders[activeIndex].tail(initialTermOffset);
        this.completedPosition.position(initialPosition);
        this.hwmPosition.position(initialPosition);
    }

    public ReceiveChannelEndpoint receiveChannelEndpoint()
    {
        return channelEndpoint;
    }

    public long correlationId()
    {
        return correlationId;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int streamId()
    {
        return streamId;
    }

    public boolean matches(final int streamId, final ReceiveChannelEndpoint channelEndpoint)
    {
        return this.streamId == streamId && this.channelEndpoint == channelEndpoint;
    }

    /**
     * Return status of the connection. Retrieved by {@link DriverConductor}.
     *
     * @return status of the connection
     */
    public Status status()
    {
        return status;
    }

    /**
     * Set status of the connection. Set by {@link DriverConductor}.
     *
     * @param status of the connection
     */
    public void status(final Status status)
    {
        this.status = status;
    }

    /**
     * Return time of last status change. Retrieved by {@link DriverConductor}.
     *
     * @return time of last status change
     */
    public long timeOfLastStatusChange()
    {
        return timeOfLastStatusChange;
    }

    /**
     * Set time of last status change. Set by {@link DriverConductor}.
     *
     * @param now timestamp to use for time
     */
    public void timeOfLastStatusChange(final long now)
    {
        timeOfLastStatusChange = now;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        completedPosition.close();
        hwmPosition.close();
        termBuffers.close();
        Stream.of(subscriberPositions).forEach(PositionIndicator::close);
    }

    /**
     * Called from the {@link DriverConductor}.
     *
     * @return if work has been done or not
     */
    public int cleanLogBuffer()
    {
        for (final LogBuffer logBuffer : rebuilders)
        {
            if (logBuffer.status() == NEEDS_CLEANING && logBuffer.compareAndSetStatus(NEEDS_CLEANING, IN_CLEANING))
            {
                logBuffer.clean();

                return 1;
            }
        }

        return 0;
    }

    /**
     * Called from the {@link DriverConductor}.
     *
     * @return if work has been done or not
     */
    public int scanForGaps()
    {
        if (scanForGapsEnabled)
        {
            return lossHandler.scan();
        }

        return 0;
    }

    /**
     * Called from the {@link DriverConductor} to determine what is remaining for the subscriber to drain.
     *
     * @return remaining bytes to drain
     */
    public long remaining()
    {
        return Math.max(completedPosition.position() - subscribersPosition(), 0);
    }

    /**
     * Insert frame into term buffer.
     *
     * @param buffer for the data frame
     * @param length of the data frame on the wire
     * @return number of bytes inserted
     */
    public int insertIntoTerm(final int termId, final int termOffset, final AtomicBuffer buffer, final int length)
    {
        int bytesInserted = 0;
        final LogRebuilder currentRebuilder = rebuilders[activeIndex];
        final int activeTermId = this.activeTermId.get();

        final long packetPosition = calculatePosition(termId, termOffset);
        final long currentPosition = position(currentRebuilder.tail());
        final long proposedPosition = packetPosition + length;

        if (isHeartbeat(currentPosition, proposedPosition) ||
            isFlowControlUnderRun(packetPosition, currentPosition) ||
            isFlowControlOverRun(proposedPosition))
        {
            return 0;
        }

        if (termId == activeTermId)
        {
            final long oldCompletedPosition = completedPosition.position();
            currentRebuilder.insert(buffer, 0, length);

            final long newCompletedPosition = lossHandler.completedPosition();
            bytesInserted = (int)(newCompletedPosition - oldCompletedPosition);
            completedPosition.position(newCompletedPosition);

            if (currentRebuilder.isComplete())
            {
                activeIndex = hwmIndex = prepareForRotation();
                this.activeTermId.lazySet(activeTermId + 1);
            }
        }
        else if (termId == (activeTermId + 1))
        {
            if (termId != hwmTermId)
            {
                hwmIndex = prepareForRotation();
                hwmTermId = termId;
            }

            rebuilders[hwmIndex].insert(buffer, 0, length);
        }

        hwmCandidate(proposedPosition);

        return bytesInserted;
    }

    /*
     * Inform the loss handler that a potentially new high position in the stream has been reached.
     */
    private void hwmCandidate(final long proposedPosition)
    {
        timeOfLastFrame.lazySet(clock.time());
        hwmPosition.position(lossHandler.hwmCandidate(proposedPosition));
    }

    /**
     * Called from the {@link DriverConductor}.
     *
     * @param now time in nanoseconds
     * @return number of work items processed.
     */
    public int sendPendingStatusMessages(final long now)
    {
        // not able to send yet because not added to dispatcher, anything received will be dropped (in progress)
        if (!statusMessagesEnabled)
        {
            return 0;
        }

        /* General approach is to check subscriber position and see if it has moved enough to warrant sending an SM.
         * - send SM when termId has moved (i.e. buffer rotation)
         * - send SM when subscriber position has moved more than the gain (min of term or window)
         * - send SM when haven't sent an SM in status message timeout
         */

        final long position = subscribersPosition();
        final int currentSmTermId = TermHelper.calculateTermIdFromPosition(position, positionBitsToShift, initialTermId);
        final int currentSmTail = TermHelper.calculateTermOffsetFromPosition(position, positionBitsToShift);

        if (0 == lastSmTimestamp || currentSmTermId != lastSmTermId ||
            (position - lastSmPosition) > currentGain || (lastSmTimestamp + statusMessageTimeout) < now)
        {
            return sendStatusMessage(currentSmTermId, currentSmTail, position, currentWindowSize, now);
        }

        // invert the work count logic. We want to appear to be less busy once we send an SM
        return 1;
    }

    private long subscribersPosition()
    {
        long position = Long.MAX_VALUE;
        for (final PositionIndicator indicator : subscriberPositions)
        {
            position = Math.min(position, indicator.position());
        }
        return position;
    }

    /**
     * Called from the {@link Receiver} thread once added to dispatcher
     */
    public void enableStatusMessages()
    {
        statusMessagesEnabled = true;
    }

    /**
     * Called from the {@link Receiver} thread once removed from dispatcher to stop sending SMs
     */
    public void disableStatusMessages()
    {
        statusMessagesEnabled = false;
    }

    /**
     * Called from the {@link Receiver} thread once removed from dispatcher to stop sending NAKs
     */
    public void disableScanForGaps()
    {
        scanForGapsEnabled = false;
    }

    /**
     * Called from the {@link DriverConductor} thread to grab the time of the last frame for liveness
     *
     * @return time of last frame from the source
     */
    public long timeOfLastFrame()
    {
        return timeOfLastFrame.get();
    }

    private int sendStatusMessage(
        final int termId, final int termOffset, final long position, final int windowSize, final long now)
    {
        statusMessageSender.send(termId, termOffset, windowSize);

        systemCounters.statusMessagesSent().orderedIncrement();
        lastSmTermId = termId;
        lastSmTimestamp = now;
        lastSmPosition = position;

        return 0;
    }

    private long position(final int currentTail)
    {
        return calculatePosition(activeTermId.get(), currentTail);
    }

    private long calculatePosition(final int termId, final int tail)
    {
        return TermHelper.calculatePosition(termId, tail, positionBitsToShift, initialTermId);
    }

    private boolean isHeartbeat(final long currentPosition, final long proposedPosition)
    {
        final boolean isHeartbeat = proposedPosition == currentPosition;

        if (isHeartbeat)
        {
            timeOfLastFrame.lazySet(clock.time());
        }

        return isHeartbeat;
    }

    private boolean isFlowControlUnderRun(final long packetPosition, final long position)
    {
        final boolean isFlowControlUnderRun = packetPosition < position;

        if (isFlowControlUnderRun)
        {
            systemCounters.flowControlUnderRuns().orderedIncrement();
        }

        return isFlowControlUnderRun;
    }

    private boolean isFlowControlOverRun(final long proposedPosition)
    {
        final long subscribersPosition = subscribersPosition();
        final boolean isFlowControlOverRun = proposedPosition > (subscribersPosition + currentWindowSize);

        if (isFlowControlOverRun)
        {
            logger.logOverRun(proposedPosition, subscribersPosition, currentWindowSize);
            systemCounters.flowControlOverRuns().orderedIncrement();
        }

        return isFlowControlOverRun;
    }

    private int prepareForRotation()
    {
        final int nextIndex = TermHelper.rotateNext(activeIndex);
        final LogRebuilder rebuilder = rebuilders[nextIndex];

        if (nextIndex != hwmIndex)
        {
            if (!ensureClean(rebuilder))
            {
                systemCounters.subscriptionCleaningLate().orderedIncrement();
            }
        }

        rebuilders[rotatePrevious(activeIndex)].statusOrdered(NEEDS_CLEANING);

        return nextIndex;
    }

    public void removeSubscription(final PositionIndicator indicator)
    {
        final PositionIndicator[] oldPositions = subscriberPositions;
        final PositionIndicator[] newPositions = new PositionIndicator[oldPositions.length - 1];
        for (int i = 0, j = 0; i < oldPositions.length; i++)
        {
            if (oldPositions[i] != indicator)
            {
                newPositions[j] = oldPositions[i];
                j++;
            }
        }
        subscriberPositions = newPositions;
    }

    public void addSubscription(final PositionIndicator indicator)
    {
        final PositionIndicator[] oldPositions = subscriberPositions;
        final int length = oldPositions.length;
        final PositionIndicator[] newPositions = new PositionIndicator[length + 1];
        System.arraycopy(oldPositions, 0, newPositions, 0, length);
        newPositions[length] = indicator;
        subscriberPositions = newPositions;
    }

}
