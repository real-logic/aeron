/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver;

import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.NanoClock;

import static io.aeron.driver.Configuration.MAX_RETRANSMITS_DEFAULT;
import static io.aeron.driver.RetransmitHandler.State.DELAYED;
import static io.aeron.driver.RetransmitHandler.State.LINGERING;

/**
 * Tracking and handling of retransmit request, NAKs, for senders, and receivers.
 * <p>
 * A max number of retransmits is permitted by {@link Configuration#MAX_RETRANSMITS_DEFAULT}. Additional received NAKs
 * will be ignored if this maximum is reached.
 */
public final class RetransmitHandler
{
    private final RetransmitAction[] retransmitActionPool = new RetransmitAction[MAX_RETRANSMITS_DEFAULT];
    private final NanoClock nanoClock;
    private final FeedbackDelayGenerator delayGenerator;
    private final FeedbackDelayGenerator lingerTimeoutGenerator;
    private final AtomicCounter invalidPackets;

    /**
     * Create a handler for the dealing with the reception of frame request a frame to be retransmitted.
     *
     * @param nanoClock              used to determine time.
     * @param invalidPackets         for recording invalid packets.
     * @param delayGenerator         to use for delay determination.
     * @param lingerTimeoutGenerator to use for linger timeout.
     */
    public RetransmitHandler(
        final NanoClock nanoClock,
        final AtomicCounter invalidPackets,
        final FeedbackDelayGenerator delayGenerator,
        final FeedbackDelayGenerator lingerTimeoutGenerator)
    {
        this.nanoClock = nanoClock;
        this.invalidPackets = invalidPackets;
        this.delayGenerator = delayGenerator;
        this.lingerTimeoutGenerator = lingerTimeoutGenerator;

        for (int i = 0; i < MAX_RETRANSMITS_DEFAULT; i++)
        {
            retransmitActionPool[i] = new RetransmitAction();
        }
    }

    /**
     * Called on reception of a NAK to start retransmits handling.
     *
     * @param termId           from the NAK and the term id of the buffer to retransmit from.
     * @param termOffset       from the NAK and the offset of the data to retransmit.
     * @param length           of the missing data.
     * @param termLength       of the term buffer.
     * @param retransmitSender to call if an immediate retransmit is required.
     */
    public void onNak(
        final int termId,
        final int termOffset,
        final int length,
        final int termLength,
        final RetransmitSender retransmitSender)
    {
        if (!isInvalid(termOffset, termLength))
        {
            final RetransmitAction action = scanForAvailableRetransmit(termId, termOffset, length);
            if (null != action)
            {
                action.termId = termId;
                action.termOffset = termOffset;
                action.length = Math.min(length, termLength - termOffset);

                final long delay = delayGenerator.generateDelayNs();
                if (0 == delay)
                {
                    retransmitSender.resend(termId, termOffset, action.length);
                    action.linger(lingerTimeoutGenerator.generateDelayNs(), nanoClock.nanoTime());
                }
                else
                {
                    action.delay(delay, nanoClock.nanoTime());
                }
            }
        }
    }

    /**
     * Called to indicate a retransmission is received that may obviate the need to send one ourselves.
     * <p>
     * NOTE: Currently only called from unit tests. Would be used for retransmitting from receivers for NAK suppression.
     *
     * @param termId     of the data.
     * @param termOffset of the data.
     */
    public void onRetransmitReceived(final int termId, final int termOffset)
    {
        final RetransmitAction action = scanForExistingRetransmit(termId, termOffset);

        if (null != action && DELAYED == action.state)
        {
            action.cancel(); // do not go into linger
        }
    }

    /**
     * Called to process any outstanding timeouts.
     *
     * @param nowNs            time in nanoseconds.
     * @param retransmitSender to call on retransmissions.
     */
    public void processTimeouts(final long nowNs, final RetransmitSender retransmitSender)
    {
        for (final RetransmitAction action : retransmitActionPool)
        {
            if (DELAYED == action.state && (action.expireNs - nowNs < 0))
            {
                retransmitSender.resend(action.termId, action.termOffset, action.length);
                action.linger(lingerTimeoutGenerator.generateDelayNs(), nanoClock.nanoTime());
            }
            else if (LINGERING == action.state && (action.expireNs - nowNs < 0))
            {
                action.cancel();
            }
        }
    }

    private boolean isInvalid(final int termOffset, final int termLength)
    {
        final boolean isInvalid = (termOffset > (termLength - DataHeaderFlyweight.HEADER_LENGTH)) || (termOffset < 0);

        if (isInvalid)
        {
            invalidPackets.increment();
        }

        return isInvalid;
    }

    private RetransmitAction scanForAvailableRetransmit(final int termId, final int termOffset, final int length)
    {
        RetransmitAction availableAction = null;
        for (final RetransmitAction action : retransmitActionPool)
        {
            switch (action.state)
            {
                case INACTIVE:
                    if (null == availableAction)
                    {
                        availableAction = action;
                    }
                    break;

                case DELAYED:
                case LINGERING:
                    if (action.termId == termId &&
                        action.termOffset <= termOffset &&
                        termOffset + length <= action.termOffset + action.length)
                    {
                        return null;
                    }
                    break;
            }
        }

        if (null != availableAction)
        {
            return availableAction;
        }

        throw new IllegalStateException("maximum number of active RetransmitActions reached");
    }

    private RetransmitAction scanForExistingRetransmit(final int termId, final int termOffset)
    {
        for (final RetransmitAction action : retransmitActionPool)
        {
            switch (action.state)
            {
                case DELAYED:
                case LINGERING:
                    if (action.termId == termId && action.termOffset == termOffset)
                    {
                        return action;
                    }
                    break;
            }
        }

        return null;
    }

    enum State
    {
        DELAYED,
        LINGERING,
        INACTIVE
    }

    static final class RetransmitAction
    {
        long expireNs;
        int termId;
        int termOffset;
        int length;
        State state = State.INACTIVE;

        void delay(final long delayNs, final long nowNs)
        {
            state = DELAYED;
            expireNs = nowNs + delayNs;
        }

        void linger(final long timeoutNs, final long nowNs)
        {
            state = LINGERING;
            expireNs = nowNs + timeoutNs;
        }

        void cancel()
        {
            state = State.INACTIVE;
        }
    }
}
