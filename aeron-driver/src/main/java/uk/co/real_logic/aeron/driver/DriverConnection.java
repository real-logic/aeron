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
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogRebuilder;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.status.PositionIndicator;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static uk.co.real_logic.aeron.common.TermHelper.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.IN_CLEANING;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.NEEDS_CLEANING;
import static uk.co.real_logic.aeron.driver.MediaDriver.SUBSCRIPTION_TERM_WINDOW_SZ;

/**
 * State maintained for active sessionIds within a channel for receiver processing
 */
public class DriverConnection implements AutoCloseable
{
    /** connection is active */
    public static final int ACTIVE = 1;

    /** connection is inactive. Publication side has timed out. */
    public static final int INACTIVE = 2;

    /** connection has been drained or timeout has occurred and is being lingered */
    public static final int LINGER = 3;

    private final ReceiveChannelEndpoint receiveChannelEndpoint;
    private final int sessionId;
    private final int streamId;
    private final TermBuffers termBuffers;
    private final PositionIndicator receiverLimit;
    private final LongSupplier clock;

    private final AtomicInteger activeTermId = new AtomicInteger();
    private final AtomicLong timeOfLastFrame = new AtomicLong();
    private int activeIndex;
    private int hwmTermId;
    private int hwmIndex;
    private int status;
    private long timeOfLastStatusChange;

    private final LogRebuilder[] rebuilders;
    private final LossHandler lossHandler;
    private final StatusMessageSender statusMessageSender;

    private final int positionBitsToShift;
    private final int initialTermId;
    private final int termWindowSize;
    private final long statusMessageTimeout;

    private long lastSmPosition;
    private long lastSmTimestamp;
    private int lastSmTermId;
    private int currentWindowSize;
    private int currentGain;

    private volatile boolean statusMessagesEnabled = false;

    public DriverConnection(final ReceiveChannelEndpoint receiveChannelEndpoint,
                            final int sessionId,
                            final int streamId,
                            final int initialTermId,
                            final int initialWindowSize,
                            final long statusMessageTimeout,
                            final TermBuffers termBuffers,
                            final LossHandler lossHandler,
                            final StatusMessageSender statusMessageSender,
                            final PositionIndicator receiverLimit,
                            final LongSupplier clock)
    {
        this.receiveChannelEndpoint = receiveChannelEndpoint;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.termBuffers = termBuffers;
        this.receiverLimit = receiverLimit;
        this.status = ACTIVE;
        this.timeOfLastStatusChange = clock.getAsLong();

        this.clock = clock;
        activeTermId.lazySet(initialTermId);
        timeOfLastFrame.lazySet(clock.getAsLong());
        this.hwmIndex = this.activeIndex = termIdToBufferIndex(initialTermId);
        this.hwmTermId = initialTermId;

        rebuilders = termBuffers.stream()
                                .map((rawLog) -> new LogRebuilder(rawLog.logBuffer(), rawLog.stateBuffer()))
                                .toArray(LogRebuilder[]::new);
        this.lossHandler = lossHandler;
        this.statusMessageSender = statusMessageSender;
        this.statusMessageTimeout = statusMessageTimeout;

        // attaching this term buffer will send an SM, so save the params set for comparison
        this.lastSmTermId = initialTermId;
        this.lastSmTimestamp = 0;

        final int termCapacity = rebuilders[0].capacity();

        // how far ahead of subscriber position to allow
        this.termWindowSize = 0 != SUBSCRIPTION_TERM_WINDOW_SZ ? SUBSCRIPTION_TERM_WINDOW_SZ : termCapacity / 2;

        // how big of a window to advertise to the publisher
        this.currentWindowSize = Math.min(termWindowSize, initialWindowSize);

        // trip of sending an SM as messages come in
        this.currentGain = Math.min(currentWindowSize / 4, termCapacity / 4);

        this.positionBitsToShift = Integer.numberOfTrailingZeros(termCapacity);
        this.initialTermId = initialTermId;
        this.lastSmPosition = TermHelper.calculatePosition(initialTermId, 0, positionBitsToShift, initialTermId);
    }

    public ReceiveChannelEndpoint receiveChannelEndpoint()
    {
        return receiveChannelEndpoint;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int streamId()
    {
        return streamId;
    }

    /**
     * Return status of the connection. Retrieved by {@link DriverConductor}.
     *
     * @return status of the connection
     */
    public int status()
    {
        return status;
    }

    /**
     * Set status of the connection. Set by {@link DriverConductor}.
     *
     * @param status of the connection
     */
    public void status(final int status)
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

    /** {@inheritDoc} */
    public void close()
    {
        termBuffers.close();
        receiverLimit.close();
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
        // if scan() returns true, loss handler moved to new GapScanner, it should be serviced soon, else be lazy
        return lossHandler.scan() ? 1 : 0;
    }

    /**
     * Called from the {@link DriverConductor} to determine what is remaining for the subscriber to drain.
     *
     * @return remaining bytes to drain
     */
    public long remaining()
    {
        // TODO: needs to account for multiple receiverLimit values (multiple subscribers) when needed
        return Math.min(lossHandler.tailPosition() - receiverLimit.position(), 0);
    }

    /**
     * Insert frame into term buffer.
     *
     * @param header for the data frame
     * @param buffer for the data frame
     * @param length of the data frame on the wire
     */
    public void insertIntoTerm(final DataHeaderFlyweight header, final AtomicBuffer buffer, final int length)
    {
        final LogRebuilder currentRebuilder = rebuilders[activeIndex];
        final int termId = header.termId();
        final int activeTermId = this.activeTermId.get();

        final int packetTail = header.termOffset();
        final long packetPosition = calculatePosition(termId, packetTail);
        final long position = position(currentRebuilder.tail());

        timeOfLastFrame.lazySet(clock.getAsLong());

        if (isOutOfBufferRange(packetPosition, length, position))
        {
            // TODO: invalid packet we probably want to update an error counter
            System.out.println(String.format("isOutOfBufferRange %x %d %x", packetPosition, length, position));
            return;
        }

        if (isBeyondFlowControlLimit(packetPosition + length))
        {
            // TODO: increment a counter to say subscriber is not keeping up
            System.out.println(String.format("isBeyondFlowControlLimit %x %d", packetPosition, length));
            return;
        }

        if (termId == activeTermId)
        {
            currentRebuilder.insert(buffer, 0, length);

            if (currentRebuilder.isComplete())
            {
                activeIndex = hwmIndex = prepareForRotation(activeTermId);
                this.activeTermId.lazySet(activeTermId + 1);
            }
        }
        else if (termId == (activeTermId + 1))
        {
            if (termId != hwmTermId)
            {
                hwmIndex = prepareForRotation(activeTermId);
                hwmTermId = termId;
                lossHandler.potentialHighPosition(packetPosition);
            }

            rebuilders[hwmIndex].insert(buffer, 0, length);
        }
    }

    /**
     * Inform the loss handler that a potentially new high position in the stream has been reached.
     *
     * @param header for the data frame
     */
    public void potentialHighPosition(final DataHeaderFlyweight header)
    {
        final long packetPosition = calculatePosition(header.termId(), header.termOffset());

        timeOfLastFrame.lazySet(clock.getAsLong());
        lossHandler.potentialHighPosition(packetPosition);
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

        /*
         * General approach is to check subscriber position and see if it has moved enough to warrant sending an SM.
         * - send SM when termId has moved (i.e. buffer rotation)
         * - send SM when subscriber position has moved more than the gain (min of term or window)
         * - send SM when haven't sent an SM in status message timeout
         */

        final long position = receiverLimit.position();
        final int currentSmTermId = TermHelper.calculateTermIdFromPosition(position, positionBitsToShift, initialTermId);
        final int currentSmTail = TermHelper.calculateTermOffsetFromPosition(position, positionBitsToShift);

        // send initial SM
        if (0 == lastSmTimestamp)
        {
            return sendStatusMessage(currentSmTermId, currentSmTail, position, currentWindowSize, now);
        }

        // if term has rotated for the subscriber position, then send an SM
        if (currentSmTermId != lastSmTermId)
        {
            return sendStatusMessage(currentSmTermId, currentSmTail, position, currentWindowSize, now);
        }

        // see if we have made enough progress to make sense to send an SM
        if ((position - lastSmPosition) > currentGain)
        {
            return sendStatusMessage(currentSmTermId, currentSmTail, position, currentWindowSize, now);
        }

        // make sure to send on timeout to prevent a stall on lost SM
        if ((lastSmTimestamp + statusMessageTimeout) < now)
        {
            return sendStatusMessage(currentSmTermId, currentSmTail, position, currentWindowSize, now);
        }

        // invert the work count logic. We want to appear to be less busy once we send an SM
        return 1;
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
     * Called from the {@link DriverConductor} thread to grab the time of the last frame for liveness
     * @return time of last frame from the source
     */
    public long timeOfLastFrame()
    {
        return timeOfLastFrame.get();
    }

    private int sendStatusMessage(final int termId,
                                  final int termOffset,
                                  final long position,
                                  final int windowSize,
                                  final long now)
    {
        statusMessageSender.send(termId, termOffset, windowSize);
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

    private boolean isBeyondFlowControlLimit(final long proposedPosition)
    {
        return proposedPosition > (receiverLimit.position() + termWindowSize);
    }

    private boolean isOutOfBufferRange(final long proposedPosition, final int length, final long currentPosition)
    {
        return proposedPosition < currentPosition || proposedPosition > (currentPosition + (termWindowSize - length));
    }

    private int prepareForRotation(final int activeTermId)
    {
        final int nextIndex = TermHelper.rotateNext(activeIndex);
        final LogRebuilder rebuilder = rebuilders[nextIndex];

        if (nextIndex != hwmIndex)
        {
            ensureClean(rebuilder, receiveChannelEndpoint.udpChannel().originalUriAsString(), streamId, activeTermId + 1);
        }

        rebuilders[rotatePrevious(activeIndex)].statusOrdered(NEEDS_CLEANING);

        return nextIndex;
    }
}
