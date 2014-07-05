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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.mediadriver.buffer.LogBuffers;
import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogRebuilder;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.StateViewer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.CLEAN_WINDOW;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.UNKNOWN_TERM_ID;

/**
 * State maintained for active sessionIds within a channel for receiver processing
 */
public class DriverConnectedSubscription
{
    /**
     * Handler for sending Status Messages (SMs)
     */
    @FunctionalInterface
    public interface SendSmHandler
    {
        /**
         * Called when an SM should be sent.
         *
         * @param termId     for the SM
         * @param termOffset for the SM
         * @param window     for the SM
         */
        void sendSm(final long termId, final int termOffset, final int window);
    }

    /** Timeout between SMs. One RTT. */
    public static final long STATUS_MESSAGE_TIMEOUT = MediaDriver.ESTIMATED_RTT_NS;

    private final InetSocketAddress srcAddress;
    private final long sessionId;
    private final long channelId;

    private final AtomicLong cleanedTermId = new AtomicLong(UNKNOWN_TERM_ID);
    private final AtomicLong currentTermId = new AtomicLong(UNKNOWN_TERM_ID);
    private int currentBufferId = 0;

    private BufferRotator rotator;
    private TermRebuilder[] rebuilders;
    private LossHandler lossHandler;

    private SendSmHandler sendSmHandler;
    private long lastSmTimestamp;
    private long lastSmTermId;
    private int lastSmTail;
    private int currentWindow;
    private int currentWindowGain;

    public DriverConnectedSubscription(final long sessionId, final long channelId, final InetSocketAddress srcAddress)
    {
        this.srcAddress = srcAddress;
        this.sessionId = sessionId;
        this.channelId = channelId;
    }

    public void termBuffer(final long initialTermId,
                           final int initialWindow,
                           final BufferRotator rotator,
                           final LossHandler lossHandler,
                           final SendSmHandler sendSmHandler)
    {
        cleanedTermId.lazySet(initialTermId + CLEAN_WINDOW);
        currentTermId.lazySet(initialTermId);
        this.rotator = rotator;
        rebuilders = rotator.buffers()
                            .map(TermRebuilder::new)
                            .toArray(TermRebuilder[]::new);
        this.lossHandler = lossHandler;
        this.sendSmHandler = sendSmHandler;

        // attaching this term buffer will send an SM, so save the params sent for comparison
        this.lastSmTermId = initialTermId;
        this.lastSmTail = lossHandler.highestContiguousOffset();
        this.currentWindow = initialWindow;
        this.currentWindowGain = currentWindow << 2; // window / 4
    }

    public InetSocketAddress sourceAddress()
    {
        return srcAddress;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public long channelId()
    {
        return channelId;
    }

    public void rebuildBuffer(final DataHeaderFlyweight header, final AtomicBuffer buffer, final long length)
    {
        final long termId = header.termId();
        final long currentTermId = this.currentTermId.get();

        if (termId == currentTermId)
        {
            final TermRebuilder rebuilder = rebuilders[currentBufferId];
            rebuilder.insert(buffer, 0, (int)length);
        }
        else if (termId == (currentTermId + 1))
        {
            this.currentTermId.lazySet(termId);
            currentBufferId = BufferRotationDescriptor.rotateId(currentBufferId);
            TermRebuilder rebuilder = rebuilders[currentBufferId];
            while (rebuilder.tailVolatile() != 0)
            {
                // TODO:
                Thread.yield();
            }

            rebuilder.insert(buffer, 0, (int)length);
        }
        else
        {
            // TODO: log or monitor this case
            System.out.println("Unexpected Term Id " + currentTermId + ":" + termId);
        }
    }

    /**
     * Called from the MediaConductor.
     *
     * @return if work has been done or not
     */
    public int processBufferRotation()
    {
        int workCount = 0;
        final long currentTermId = this.currentTermId.get();
        final long expectedTermId = currentTermId + CLEAN_WINDOW;
        final long cleanedTermId = this.cleanedTermId.get();

        // TODO: I don't think this works like it should be working... + had to add check on cleanedTermId for unknown
        if (currentTermId != UNKNOWN_TERM_ID && cleanedTermId != UNKNOWN_TERM_ID && expectedTermId > cleanedTermId)
        {
            try
            {
                rotator.rotate();
                this.cleanedTermId.lazySet(cleanedTermId + 1);
                ++workCount;
            }
            catch (final IOException ex)
            {
                // TODO; log
                ex.printStackTrace();
            }
        }

        return workCount;
    }

    /**
     * Called from the MediaConductor.
     *
     * @return if work has been done or not
     */
    public int scanForGaps()
    {
        if (null != lossHandler)
        {
            lossHandler.scan();
        }

        // scan lazily
        return 0;
    }

    /**
     * Called form the MediaConductor.
     *
     * @return number of work items processed.
     */
    public int sendAnyPendingSm(final long currentTimestamp)
    {
        if (null == lossHandler || null == sendSmHandler)
        {
            return 0;
        }

        /*
         * General approach is to check tail and see if it has moved enough to warrant sending an SM.
         * - send SM when termId has moved (i.e. buffer rotation of LossHandler - i.e. term completed)
         * - send SM when (currentTail - lastSmTail) > X% of window (the window gain)
         * - send SM when currentTail > lastTail && timeOfLastSM too long
         */

        final int currentSmTail = lossHandler.highestContiguousOffset();
        final long currentSmTermId = lossHandler.currentTermId();

        // if term has rotated for loss handler, then send an SM
        if (lossHandler.currentTermId() != lastSmTermId)
        {
            lastSmTimestamp = currentTimestamp;
            return sendSm(currentSmTermId, currentSmTail, currentWindow);
        }

        // made progress since last time we sent an SM, so may send
        if (currentSmTail > lastSmTail)
        {
            // see if we have made enough progress to make sense to send an SM
            if ((currentSmTail - lastSmTail) > currentWindowGain)
            {
                lastSmTimestamp = currentTimestamp;
                return sendSm(currentSmTermId, currentSmTail, currentWindow);
            }

            // lastSmTimestamp might be 0 due to being initialized, but if we have sent some, then fine.
//            if (currentTimestamp > (lastSmTimestamp + STATUS_MESSAGE_TIMEOUT) && lastSmTimestamp > 0)
//            {
//                lastSmTimestamp = currentTimestamp;
//                return sendSm(currentSmTermId, currentSmTail, currentWindow);
//            }
        }

        // invert the work count logic. We want to appear to be less busy once we send an SM
        return 1;
    }

    private int sendSm(final long termId, final int termOffset, final int window)
    {
        sendSmHandler.sendSm(termId, termOffset, window);
        lastSmTermId = termId;
        lastSmTail = termOffset;

        return 0;
    }

    private static class TermRebuilder
    {
        private final LogRebuilder logRebuilder;
        private final StateViewer stateViewer;

        public TermRebuilder(final LogBuffers buffer)
        {
            final AtomicBuffer stateBuffer = buffer.stateBuffer();
            stateViewer = new StateViewer(stateBuffer);
            logRebuilder = new LogRebuilder(buffer.logBuffer(), stateBuffer);
        }

        public int tailVolatile()
        {
            return stateViewer.tailVolatile();
        }

        public void insert(final AtomicBuffer buffer, final int offset, final int length)
        {
            logRebuilder.insert(buffer, offset, length);
        }
    }
}
