/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static org.agrona.SystemUtil.getDurationInNanos;

/**
 * Minimum multicast sender flow control strategy.
 * <p>
 * Flow control is set to minimum of tracked receivers.
 * <p>
 * Tracking of receivers is done as long as they continue to send Status Messages. Once SMs stop, the receiver tracking
 * for that receiver will timeout after a given number of nanoseconds.
 */
public class MinMulticastFlowControl implements FlowControl
{
    /**
     * Property name to set timeout, in nanoseconds, for a receiver to be tracked.
     */
    public static final String RECEIVER_TIMEOUT_PROP_NAME = "aeron.MinMulticastFlowControl.receiverTimeout";

    /**
     * Default timeout, in nanoseconds, until a receiver is no longer tracked and considered for minimum.
     */
    public static final long RECEIVER_TIMEOUT_DEFAULT = TimeUnit.SECONDS.toNanos(2);

    public static final long RECEIVER_TIMEOUT = getDurationInNanos(
        RECEIVER_TIMEOUT_PROP_NAME, RECEIVER_TIMEOUT_DEFAULT);

    static final Receiver[] EMPTY_RECEIVERS = new Receiver[0];
    private Receiver[] receivers = EMPTY_RECEIVERS;

    /**
     * {@inheritDoc}
     */
    public void initialize(final int initialTermId, final int termBufferLength)
    {
    }

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

        final long windowLength = flyweight.receiverWindowLength();
        final long receiverId = flyweight.receiverId();
        boolean isExisting = false;
        long minPosition = Long.MAX_VALUE;

        for (final Receiver receiver : receivers)
        {
            if (receiverId == receiver.receiverId)
            {
                receiver.lastPosition = Math.max(position, receiver.lastPosition);
                receiver.lastPositionPlusWindow = position + windowLength;
                receiver.timeOfLastStatusMessageNs = timeNs;
                isExisting = true;
            }

            minPosition = Math.min(minPosition, receiver.lastPositionPlusWindow);
        }

        if (!isExisting)
        {
            final Receiver receiver = new Receiver(
                position, position + windowLength, timeNs, receiverId, receiverAddress);
            receivers = add(receivers, receiver);
            minPosition = Math.min(minPosition, position + windowLength);
        }

        return Math.max(senderLimit, minPosition);
    }

    /**
     * {@inheritDoc}
     */
    public long onIdle(final long timeNs, final long senderLimit, final long senderPosition, final boolean isEos)
    {
        long minLimitPosition = Long.MAX_VALUE;
        int removed = 0;

        for (int lastIndex = receivers.length - 1, i = lastIndex; i >= 0; i--)
        {
            final Receiver receiver = receivers[i];
            if ((receiver.timeOfLastStatusMessageNs + RECEIVER_TIMEOUT) - timeNs < 0)
            {
                if (i != lastIndex)
                {
                    receivers[i] = receivers[lastIndex--];
                }
                removed++;
            }
            else
            {
                minLimitPosition = Math.min(minLimitPosition, receiver.lastPositionPlusWindow);
            }
        }

        if (removed > 0)
        {
            receivers = truncateReceivers(receivers, removed);
        }

        return receivers.length > 0 ? minLimitPosition : senderLimit;
    }

    static Receiver[] add(final Receiver[] receivers, final Receiver receiver)
    {
        final int length = receivers.length;
        final Receiver[] newElements = new Receiver[length + 1];

        System.arraycopy(receivers, 0, newElements, 0, length);
        newElements[length] = receiver;
        return newElements;
    }

    static Receiver[] truncateReceivers(final Receiver[] receivers, final int removed)
    {
        final int length = receivers.length;
        final int newLength = length - removed;

        if (0 == newLength)
        {
            return EMPTY_RECEIVERS;
        }
        else
        {
            final Receiver[] newElements = new Receiver[newLength];
            System.arraycopy(receivers, 0, newElements, 0, newLength);
            return newElements;
        }
    }

    static class Receiver
    {
        long lastPosition;
        long lastPositionPlusWindow;
        long timeOfLastStatusMessageNs;
        final long receiverId;
        final InetSocketAddress address;

        Receiver(
            final long lastPosition,
            final long lastPositionPlusWindow,
            final long timeNs,
            final long receiverId,
            final InetSocketAddress receiverAddress)
        {
            this.lastPosition = lastPosition;
            this.lastPositionPlusWindow = lastPositionPlusWindow;
            this.timeOfLastStatusMessageNs = timeNs;
            this.receiverId = receiverId;
            this.address = receiverAddress;
        }
    }
}
