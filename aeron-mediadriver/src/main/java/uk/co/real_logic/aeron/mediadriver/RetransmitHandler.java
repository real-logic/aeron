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
 * A max number of retransmits is allowed at one time. Additional received NAKs will be
 * ignored if this maximum is reached. Each retransmit will have 1 timer.
 */
public class RetransmitHandler
{
    /** Maximum number of concurrent retransmits */
    public static final int MAX_RETRANSMITS = MediaDriver.MAX_RETRANSMITS_DEFAULT;

    private final LogReader reader;
    private final TimerWheel wheel;
    private final LogReader.FrameHandler sendRetransmitHandler;
    private final Queue<Retransmit> inActive;
    private final Int2ObjectHashMap<Retransmit> activeMap;
    private final FeedbackDelayGenerator delayGenerator;

    /**
     * Create a retransmit handler for a log buffer.
     *
     * @param reader to read frames from for retransmission
     * @param wheel for timers
     * @param delayGenerator to use for delay determination
     * @param retransmitHandler for sending retransmits
     */
    public RetransmitHandler(final LogReader reader,
                             final TimerWheel wheel,
                             final FeedbackDelayGenerator delayGenerator,
                             final LogReader.FrameHandler retransmitHandler)
    {
        this.reader = reader;
        this.wheel = wheel;
        this.delayGenerator = delayGenerator;
        this.sendRetransmitHandler = retransmitHandler;

        this.inActive = new OneToOneConcurrentArrayQueue<>(MAX_RETRANSMITS);
        this.activeMap = new Int2ObjectHashMap<>();

        IntStream.range(0, MAX_RETRANSMITS).forEach((i) -> this.inActive.offer(new Retransmit()));
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
        if (inActive.size() > 0 && null == activeMap.get(termOffset))
        {
            final Retransmit rx = inActive.poll();
            final long delay = determineRetransmitDelay();

            rx.termOffset = termOffset;

            if (0 == delay)
            {
                retransmit(rx);
                rx.linger(determineLingerTimeout());
            }
            else
            {
                rx.delay(delay);
            }

            activeMap.put(termOffset, rx);
        }
    }

    /**
     * Called to indicate a retransmission is received that may obviate the need to send one ourselves.
     *
     * @param termOffset of the data
     */
    public void onRetransmitReceived(final int termOffset)
    {
        final Retransmit rx = activeMap.get(termOffset);

        // suppress sending retransmit only if we are delaying
        if (null != rx && State.DELAYED == rx.state)
        {
            activeMap.remove(termOffset);
            rx.state = State.INACTIVE;
            inActive.offer(rx);
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

    private void retransmit(final Retransmit rx)
    {
        reader.seek(rx.termOffset);
        reader.read(sendRetransmitHandler);
    }

    public enum State
    {
        DELAYED,
        LINGERING,
        INACTIVE
    }

    public class Retransmit
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
                delayTimer = wheel.newTimeout(delay, TimeUnit.NANOSECONDS, this::onDelayTimeout);
            }
            else
            {
                wheel.rescheduleTimeout(delay, TimeUnit.NANOSECONDS, delayTimer);
            }
        }

        public void linger(final long timeout)
        {
            state = State.LINGERING;
            if (null == lingerTimer)
            {
                lingerTimer = wheel.newTimeout(timeout, TimeUnit.NANOSECONDS, this::onLingerTimeout);
            }
            else
            {
                wheel.rescheduleTimeout(timeout, TimeUnit.NANOSECONDS, lingerTimer);
            }
        }

        public void onDelayTimeout()
        {
            retransmit(this);
            linger(determineLingerTimeout());
        }

        public void onLingerTimeout()
        {
            state = State.INACTIVE;
            activeMap.remove(termOffset);
            inActive.offer(this);
        }
    }
}
