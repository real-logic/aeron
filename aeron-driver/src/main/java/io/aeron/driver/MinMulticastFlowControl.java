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
import org.agrona.collections.ArrayListUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
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
    private static final String RECEIVER_TIMEOUT_PROP_NAME = "aeron.MinMulticastFlowControl.receiverTimeout";

    /**
     * Default timeout, in nanoseconds, until a receiver is no longer tracked and considered for minimum.
     */
    private static final long RECEIVER_TIMEOUT_DEFAULT = TimeUnit.SECONDS.toNanos(2);

    private static final long RECEIVER_TIMEOUT = getDurationInNanos(
        RECEIVER_TIMEOUT_PROP_NAME, RECEIVER_TIMEOUT_DEFAULT);

    private final ArrayList<Receiver> receiverList = new ArrayList<>();

    private volatile boolean shouldLinger = true;

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

        final ArrayList<Receiver> receiverList = this.receiverList;
        for (int i = 0, size = receiverList.size(); i < size; i++)
        {
            final Receiver receiver = receiverList.get(i);

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
            receiverList.add(new Receiver(
                position, position + windowLength, timeNs, receiverId, receiverAddress));
            minPosition = Math.min(minPosition, position + windowLength);
        }

        return Math.max(senderLimit, minPosition);
    }

    /**
     * {@inheritDoc}
     */
    public long onIdle(
        final long timeNs, final long senderLimit, final long senderPosition, final boolean isEndOfStream)
    {
        long minPosition = Long.MAX_VALUE;
        long minLimitPosition = Long.MAX_VALUE;
        final ArrayList<Receiver> receiverList = this.receiverList;

        for (int lastIndex = receiverList.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Receiver receiver = receiverList.get(i);
            if ((receiver.timeOfLastStatusMessageNs + RECEIVER_TIMEOUT) - timeNs < 0)
            {
                ArrayListUtil.fastUnorderedRemove(receiverList, i, lastIndex--);
            }
            else
            {
                minPosition = Math.min(minPosition, receiver.lastPosition);
                minLimitPosition = Math.min(minLimitPosition, receiver.lastPositionPlusWindow);
            }
        }

        if (isEndOfStream && shouldLinger)
        {
            if (0 == receiverList.size() || minPosition >= senderPosition)
            {
                shouldLinger = false;
            }
        }

        return receiverList.size() > 0 ? minLimitPosition : senderLimit;
    }

    /**
     * {@inheritDoc}
     */
    public boolean shouldLinger(final long timeNs)
    {
        return shouldLinger;
    }

    static class Receiver
    {
        long lastPosition;
        long lastPositionPlusWindow;
        long timeOfLastStatusMessageNs;
        long receiverId;
        InetSocketAddress address;

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