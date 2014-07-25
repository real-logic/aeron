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

import static uk.co.real_logic.aeron.common.TermHelper.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.IN_CLEANING;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.NEEDS_CLEANING;

/**
 * State maintained for active sessionIds within a channel for receiver processing
 */
public class DriverConnectedSubscription implements AutoCloseable
{
    private static final int STATE_CREATED = 0;
    private static final int STATE_READY_TO_SEND_SMS = 1;

    private final UdpDestination udpDestination;
    private final int sessionId;
    private final int channelId;
    private TermBuffers termBuffers;
    private PositionIndicator subscriberLimit;

    private final AtomicInteger activeTermId = new AtomicInteger();
    private int activeIndex;
    private int hwmTermId;
    private int hwmIndex;

    private final LogRebuilder[] rebuilders;
    private final LossHandler lossHandler;
    private final StatusMessageSender statusMessageSender;

    private final int positionBitsToShift;
    private final int initialTermId;
    private final int bufferLimit;

    private long lastSmTimestamp;
    private long lastSmTermId;
    private int lastSmTail;
    private int currentWindowSize;
    private int currentWindowGain;
    private int termSizeSmGain;

    private AtomicInteger state = new AtomicInteger(STATE_CREATED);

    public DriverConnectedSubscription(final UdpDestination udpDestination,
                                       final int sessionId,
                                       final int channelId,
                                       final int initialTermId,
                                       final int initialWindow,
                                       final TermBuffers termBuffers,
                                       final LossHandler lossHandler,
                                       final StatusMessageSender statusMessageSender,
                                       final PositionIndicator subscriberLimit)
    {
        this.udpDestination = udpDestination;
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.termBuffers = termBuffers;
        this.subscriberLimit = subscriberLimit;

        activeTermId.lazySet(initialTermId);
        this.hwmIndex = this.activeIndex = termIdToBufferIndex(initialTermId);
        this.hwmTermId = initialTermId;

        rebuilders = termBuffers.stream()
                                .map((rawLog) -> new LogRebuilder(rawLog.logBuffer(), rawLog.stateBuffer()))
                                .toArray(LogRebuilder[]::new);
        this.lossHandler = lossHandler;
        this.statusMessageSender = statusMessageSender;

        // attaching this term buffer will send an SM, so save the params set for comparison
        this.lastSmTermId = initialTermId;
        this.lastSmTail = lossHandler.highestContiguousOffset();
        this.lastSmTimestamp = 0;

        final int termCapacity = rebuilders[0].capacity();
        this.currentWindowSize = initialWindow;
        this.currentWindowGain = currentWindowSize << 2; // window / 4
        this.bufferLimit = termCapacity / 2;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(termCapacity);
        this.initialTermId = initialTermId;
        this.termSizeSmGain = termCapacity / 4;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int channelId()
    {
        return channelId;
    }

    public void close()
    {
        termBuffers.close();
        subscriberLimit.close();
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

    public void insertIntoTerm(final DataHeaderFlyweight header, final AtomicBuffer buffer, final int length)
    {
        final LogRebuilder currentRebuilder = rebuilders[activeIndex];
        final int termId = header.termId();
        final int activeTermId = this.activeTermId.get();

        final int packetTail = header.termOffset();
        final long packetPosition = calculatePosition(termId, packetTail);
        final long position = position(currentRebuilder.tail());

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
            currentRebuilder.insert(buffer, 0, (int)length);

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
            }

            rebuilders[hwmIndex].insert(buffer, 0, (int)length);
        }
    }

    /**
     * Called from the {@link DriverConductor}.
     *
     * @param now time in nanoseconds
     * @return number of work items processed.
     */
    public int sendPendingStatusMessages(final long now)
    {
        /*
         * General approach is to check tail and see if it has moved enough to warrant sending an SM.
         * - send SM when termId has moved (i.e. buffer rotation of LossHandler - i.e. term completed)
         * - send SM when (currentTail - lastSmTail) > X% of window (the window gain)
         * - send SM when currentTail > lastTail && timeOfLastSM too long
         */

        final int currentSmTail = lossHandler.highestContiguousOffset();
        final int currentSmTermId = lossHandler.activeTermId();

        // not able to send yet because not added to dispatcher, anything received will be dropped (in progress)
        if (STATE_CREATED == state.get())
        {
            return 0;
        }

        // send initial SM
        if (0 == lastSmTimestamp)
        {
            lastSmTimestamp = now;
            return sendStatusMessage(currentSmTermId, currentSmTail, currentWindowSize);
        }

        // if term has rotated for loss handler, then send an SM
        if (lossHandler.activeTermId() != lastSmTermId)
        {
            lastSmTimestamp = now;
            return sendStatusMessage(currentSmTermId, currentSmTail, currentWindowSize);
        }

        // made progress since last time we sent an SM, so may send
        if (currentSmTail > lastSmTail)
        {
            // see if we have made enough progress to make sense to send an SM
            if ((currentSmTail - lastSmTail) > currentWindowGain)
            {
                lastSmTimestamp = now;
                return sendStatusMessage(currentSmTermId, currentSmTail, currentWindowSize);
            }

            if ((currentSmTail - lastSmTail) > termSizeSmGain)
            {
                lastSmTimestamp = now;
                return sendStatusMessage(currentSmTermId, currentSmTail, currentWindowSize);
            }

            // lastSmTimestamp might be 0 due to being initialized, but if we have sent some, then fine.
//            if (now > (lastSmTimestamp + STATUS_MESSAGE_TIMEOUT) && lastSmTimestamp > 0)
//            {
//                lastSmTimestamp = now;
//                return send(currentSmTermId, currentSmTail, currentWindowSize);
//            }
        }

        // invert the work count logic. We want to appear to be less busy once we send an SM
        return 1;
    }

    /**
     * Called from the {@link Receiver} thread once added to dispatcher
     */
    public void readyToSendSms()
    {
        state.lazySet(STATE_READY_TO_SEND_SMS);
    }

    private int sendStatusMessage(final int termId, final int termOffset, final int windowSize)
    {
        statusMessageSender.send(termId, termOffset, windowSize);
        lastSmTermId = termId;
        lastSmTail = termOffset;

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
        return proposedPosition > (subscriberLimit.position() + bufferLimit);
    }

    public boolean isOutOfBufferRange(final long proposedPosition, final int length, final long currentPosition)
    {
        return proposedPosition < currentPosition || proposedPosition > (currentPosition + (bufferLimit - length));
    }

    private int prepareForRotation(final int activeTermId)
    {
        final int nextIndex = TermHelper.rotateNext(activeIndex);
        final LogRebuilder rebuilder = rebuilders[nextIndex];

        if (nextIndex != hwmIndex)
        {
            ensureClean(rebuilder, udpDestination.originalUriAsString(), channelId, activeTermId + 1);
        }

        rebuilders[rotatePrevious(activeIndex)].statusOrdered(NEEDS_CLEANING);

        return nextIndex;
    }
}
