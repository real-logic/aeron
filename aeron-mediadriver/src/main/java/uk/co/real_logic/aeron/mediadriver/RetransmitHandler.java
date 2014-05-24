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

import uk.co.real_logic.aeron.util.FeedbackDelayGenerator;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.collections.Int2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;

import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Tracking and handling of retransmit request, NAKs, for senders and receivers
 *
 * A max number of retransmits is permitted by {@link #MAX_RETRANSMITS}. Additional received NAKs will be
 * ignored if this maximum is reached. Each retransmit will have 1 timer.
 */
public class RetransmitHandler
{
    /** Maximum number of concurrent retransmits */
    public static final int MAX_RETRANSMITS = MediaDriver.MAX_RETRANSMITS_DEFAULT;

    private final LogReader reader;
    private final TimerWheel timerWheel;
    private final LogReader.FrameHandler sendRetransmitHandler;
    private final Queue<RetransmitRequest> retransmitRequestPool = new OneToOneConcurrentArrayQueue<>(MAX_RETRANSMITS);
    private final Int2ObjectHashMap<RetransmitRequest> activeRequestByTermOffsetMap = new Int2ObjectHashMap<>();
    private final FeedbackDelayGenerator delayGenerator;

    /**
     * Create a retransmit handler for a log buffer.
     *
     * @param reader to read frames from for retransmission
     * @param timerWheel for timers
     * @param delayGenerator to use for delay determination
     * @param retransmitHandler for sending retransmits
     */
    public RetransmitHandler(final LogReader reader,
                             final TimerWheel timerWheel,
                             final FeedbackDelayGenerator delayGenerator,
                             final LogReader.FrameHandler retransmitHandler)
    {
        this.reader = reader;
        this.timerWheel = timerWheel;
        this.delayGenerator = delayGenerator;
        this.sendRetransmitHandler = retransmitHandler;

        IntStream.range(0, MAX_RETRANSMITS).forEach((i) -> retransmitRequestPool.offer(new RetransmitRequest()));
    }

    /**
     * Called on reception of a NAK to start retransmits handling.
     *
     * @param termOffset from the NAK and the offset of the data to retransmit
     */
    public void onNak(final int termOffset)
    {
        // only handle the NAK if we have a free Retransmit to store the state and we aren't holding
        // state for the offset already
        if (retransmitRequestPool.size() > 0 && null == activeRequestByTermOffsetMap.get(termOffset))
        {
            final RetransmitRequest retransmitRequest = retransmitRequestPool.poll();
            final long delay = determineRetransmitDelay();

            retransmitRequest.termOffset = termOffset;

            if (0 == delay)
            {
                send(retransmitRequest);
                retransmitRequest.linger(determineLingerTimeout());
            }
            else
            {
                retransmitRequest.delay(delay);
            }

            activeRequestByTermOffsetMap.put(termOffset, retransmitRequest);
        }
    }

    /**
     * Called to indicate a retransmission is received that may obviate the need to send one ourselves.
     *
     * @param termOffset of the data
     */
    public void onRetransmitReceived(final int termOffset)
    {
        final RetransmitRequest retransmitRequest = activeRequestByTermOffsetMap.get(termOffset);

        // suppress sending retransmit only if we are delaying
        if (null != retransmitRequest && State.DELAYED == retransmitRequest.state)
        {
            activeRequestByTermOffsetMap.remove(termOffset);
            retransmitRequest.state = State.INACTIVE;
            retransmitRequestPool.offer(retransmitRequest);
        }
    }

    private long determineRetransmitDelay()
    {
        return delayGenerator.generateDelay();
    }

    private long determineLingerTimeout()
    {
        // TODO: grab value from MediaDriver config
        return TimeUnit.MILLISECONDS.toNanos(10);
    }

    private void send(final RetransmitRequest retransmitRequest)
    {
        reader.seek(retransmitRequest.termOffset);
        reader.read(sendRetransmitHandler);
    }

    private enum State
    {
        DELAYED,
        LINGERING,
        INACTIVE
    }

    private class RetransmitRequest
    {
        private int termOffset;
        private State state = State.INACTIVE;
        private TimerWheel.Timer delayTimer;
        private TimerWheel.Timer lingerTimer;

        public void delay(final long delay)
        {
            state = State.DELAYED;
            if (null == delayTimer)
            {
                delayTimer = timerWheel.newTimeout(delay, TimeUnit.NANOSECONDS, this::onDelayTimeout);
            }
            else
            {
                timerWheel.rescheduleTimeout(delay, TimeUnit.NANOSECONDS, delayTimer);
            }
        }

        public void linger(final long timeout)
        {
            state = State.LINGERING;
            if (null == lingerTimer)
            {
                lingerTimer = timerWheel.newTimeout(timeout, TimeUnit.NANOSECONDS, this::onLingerTimeout);
            }
            else
            {
                timerWheel.rescheduleTimeout(timeout, TimeUnit.NANOSECONDS, lingerTimer);
            }
        }

        public void onDelayTimeout()
        {
            send(this);
            linger(determineLingerTimeout());
        }

        public void onLingerTimeout()
        {
            state = State.INACTIVE;
            activeRequestByTermOffsetMap.remove(termOffset);
            retransmitRequestPool.offer(this);
        }
    }
}
