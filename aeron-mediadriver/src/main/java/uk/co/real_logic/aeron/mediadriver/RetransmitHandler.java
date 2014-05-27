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
    private final Queue<RetransmitAction> retransmitActionPool = new OneToOneConcurrentArrayQueue<>(MAX_RETRANSMITS);
    private final Int2ObjectHashMap<RetransmitAction> activeRetransmitByTermOffsetMap = new Int2ObjectHashMap<>();
    private final FeedbackDelayGenerator delayGenerator;
    private final FeedbackDelayGenerator lingerTimeoutGenerator;

    /**
     * Create a retransmit handler for a log buffer.
     *
     * @param reader to read frames from for retransmission
     * @param timerWheel for timers
     * @param delayGenerator to use for delay determination
     * @param lingerTimeoutGenerator to use for linger timeout
     * @param retransmitHandler for sending retransmits
     */
    public RetransmitHandler(final LogReader reader,
                             final TimerWheel timerWheel,
                             final FeedbackDelayGenerator delayGenerator,
                             final FeedbackDelayGenerator lingerTimeoutGenerator,
                             final LogReader.FrameHandler retransmitHandler)
    {
        this.reader = reader;
        this.timerWheel = timerWheel;
        this.delayGenerator = delayGenerator;
        this.lingerTimeoutGenerator = lingerTimeoutGenerator;
        this.sendRetransmitHandler = retransmitHandler;

        IntStream.range(0, MAX_RETRANSMITS).forEach((i) -> retransmitActionPool.offer(new RetransmitAction()));
    }

    /**
     * Called on reception of a NAK to start retransmits handling.
     *
     * @param termOffset from the NAK and the offset of the data to retransmit
     */
    public void onNak(final int termOffset)
    {
        if (!retransmitActionPool.isEmpty() &&
            null == activeRetransmitByTermOffsetMap.get(termOffset) &&
            reader.tailVolatile() > termOffset)
        {
            final RetransmitAction retransmitAction = retransmitActionPool.poll();
            retransmitAction.termOffset = termOffset;

            final long delay = determineRetransmitDelay();
            if (0 == delay)
            {
                perform(retransmitAction);
                retransmitAction.linger(determineLingerTimeout());
            }
            else
            {
                retransmitAction.delay(delay);
            }

            activeRetransmitByTermOffsetMap.put(termOffset, retransmitAction);
        }
    }

    /**
     * Called to indicate a retransmission is received that may obviate the need to send one ourselves.
     *
     * @param termOffset of the data
     */
    public void onRetransmitReceived(final int termOffset)
    {
        final RetransmitAction retransmitAction = activeRetransmitByTermOffsetMap.get(termOffset);

        if (null != retransmitAction && State.DELAYED == retransmitAction.state)
        {
            activeRetransmitByTermOffsetMap.remove(termOffset);
            retransmitAction.state = State.INACTIVE;
            retransmitActionPool.offer(retransmitAction);
            retransmitAction.delayTimer.cancel();
            // do not go into linger
        }
    }

    private long determineRetransmitDelay()
    {
        return delayGenerator.generateDelay();
    }

    private long determineLingerTimeout()
    {
        return lingerTimeoutGenerator.generateDelay();
    }

    private void perform(final RetransmitAction retransmitAction)
    {
        reader.seek(retransmitAction.termOffset);
        reader.read(sendRetransmitHandler);
    }

    private enum State
    {
        DELAYED,
        LINGERING,
        INACTIVE
    }

    private class RetransmitAction
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
            perform(this);
            linger(determineLingerTimeout());
        }

        public void onLingerTimeout()
        {
            state = State.INACTIVE;
            activeRetransmitByTermOffsetMap.remove(termOffset);
            retransmitActionPool.offer(this);
        }
    }
}
