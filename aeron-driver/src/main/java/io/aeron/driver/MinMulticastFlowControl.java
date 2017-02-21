/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import io.aeron.ArrayListUtil;
import io.aeron.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;

/**
 * Minimum multicast sender flow control strategy.
 *
 * Flow control is set to minimum of tracked receivers.
 *
 * Tracking of receivers is done as long as they continue to send Status Messages. Once SMs stop, the receiver tracking
 * for that receiver will timeout after a given number of nanoseconds.
 */
public class MinMulticastFlowControl implements FlowControl
{
    /**
     * Property name to set timeout, in nanoseconds, for a receiver to be tracked.
     */
    private static final String RECEIVER_TIMEOUT_PROP_NAME = "aeron.MinMulticastFlowControl.receiverTimeout";

    /**
     * Default timeout, in nanoseconds, until a receiver is no longer tracked and considered for minimum.
     */
    private static final long RECEIVER_TIMEOUT_DEFAULT = TimeUnit.SECONDS.toNanos(2);

    private static final long RECEIVER_TIMEOUT = Long.getLong(RECEIVER_TIMEOUT_PROP_NAME, RECEIVER_TIMEOUT_DEFAULT);

    private final ArrayList<Receiver> receiverList = new ArrayList<>();

    /**
     * {@inheritDoc}
     */
    public long onStatusMessage(
        final StatusMessageFlyweight flyweight,
        final InetSocketAddress receiverAddress,
        final long senderLimit,
        final int initialTermId,
        final int positionBitsToShift,
        final long now)
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

        final ArrayList<Receiver> receiverList = this.receiverList;
        for (int i = 0, size = receiverList.size(); i < size; i++)
        {
            final Receiver receiver = receiverList.get(i);

            if (receiverId == receiver.receiverId)
            {
                receiver.lastPositionPlusWindow = position + windowLength;
                receiver.timeOfLastStatusMessage = now;
                isExisting = true;
            }

            minPosition = Math.min(minPosition, receiver.lastPositionPlusWindow);
        }

        if (!isExisting)
        {
            receiverList.add(new Receiver(position + windowLength, now, receiverId, receiverAddress));
            minPosition = Math.min(minPosition, position + windowLength);
        }

        return Math.max(senderLimit, minPosition);
    }

    /**
     * {@inheritDoc}
     */
    public void initialize(final int initialTermId, final int termBufferCapacity)
    {
    }

    /**
     * {@inheritDoc}
     */
    public long onIdle(final long now, final long senderLimit)
    {
        long minPosition = Long.MAX_VALUE;
        final ArrayList<Receiver> receiverList = this.receiverList;

        for (int lastIndex = receiverList.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Receiver receiver = receiverList.get(i);
            if (now > (receiver.timeOfLastStatusMessage + RECEIVER_TIMEOUT))
            {
                ArrayListUtil.fastUnorderedRemove(receiverList, i, lastIndex);
                lastIndex--;
            }
            else
            {
                minPosition = Math.min(minPosition, receiver.lastPositionPlusWindow);
            }
        }

        return receiverList.size() > 0 ? minPosition : senderLimit;
    }

    static class Receiver
    {
        long lastPositionPlusWindow;
        long timeOfLastStatusMessage;
        long receiverId;
        InetSocketAddress address;

        Receiver(
            final long lastPositionPlusWindow,
            final long now,
            final long receiverId,
            final InetSocketAddress receiverAddress)
        {
            this.lastPositionPlusWindow = lastPositionPlusWindow;
            this.timeOfLastStatusMessage = now;
            this.receiverId = receiverId;
            this.address = receiverAddress;
        }
    }
}