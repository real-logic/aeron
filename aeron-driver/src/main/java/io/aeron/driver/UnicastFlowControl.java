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

import io.aeron.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;

/**
 * Default unicast sender flow control strategy.
 * <p>
 * Max of right edges.
 * No tracking of receivers.
 */
public class UnicastFlowControl implements FlowControl
{
    /**
     * Timeout, in nanoseconds, until a receiver is no longer tracked and considered for linger purposes.
     */
    private static final long RECEIVER_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(2);

    private long lastPosition = 0;
    private long timeOfLastStatusMessage = 0;

    private volatile boolean shouldLinger = true;

    /**
     * {@inheritDoc}
     */
    public long onStatusMessage(
        final StatusMessageFlyweight flyweight,
        final InetSocketAddress receiverAddress,
        final long senderLimit,
        final int initialTermId,
        final int positionBitsToShift,
        final long timeNs)
    {
        final long position = computePosition(
            flyweight.consumptionTermId(),
            flyweight.consumptionTermOffset(),
            positionBitsToShift,
            initialTermId);

        lastPosition = Math.max(lastPosition, position);
        timeOfLastStatusMessage = timeNs;

        return Math.max(senderLimit, position + flyweight.receiverWindowLength());
    }

    /**
     * {@inheritDoc}
     */
    public void initialize(final int initialTermId, final int termBufferLength)
    {
    }

    /**
     * {@inheritDoc}
     */
    public long onIdle(final long timeNs, final long senderLimit, final long senderPosition, final boolean isEos)
    {
        if (isEos && shouldLinger)
        {
            if (lastPosition >= senderPosition || ((timeOfLastStatusMessage + RECEIVER_TIMEOUT_NS) - timeNs < 0))
            {
                shouldLinger = false;
            }
        }

        return senderLimit;
    }

    /**
     * {@inheritDoc}
     */
    public boolean shouldLinger(final long timeNs)
    {
        return shouldLinger;
    }
}
