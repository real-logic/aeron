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

import io.aeron.CommonContext;
import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.StatusMessageFlyweight;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static org.agrona.AsciiEncoding.parseIntAscii;
import static org.agrona.AsciiEncoding.parseLongAscii;
import static org.agrona.SystemUtil.parseDuration;

/**
 * Abstract minimum multicast sender flow control strategy. It supports the concept of only tracking the minimum of a
 * group of receivers, not all possible receivers. However, it is agnostic of how that group is determined.
 * <p>
 * Tracking of receivers is done as long as they continue to send Status Messages. Once SMs stop, the receiver tracking
 * for that receiver will timeout after a given number of nanoseconds.
 */
public abstract class AbstractMinMulticastFlowControl implements FlowControl
{
    static final Receiver[] EMPTY_RECEIVERS = new Receiver[0];

    private long receiverTimeoutNs;
    private long groupTag;
    private int groupMinSize;
    private final boolean isGroupTagAware;
    private volatile Receiver[] receivers = EMPTY_RECEIVERS;

    public AbstractMinMulticastFlowControl(final boolean isGroupTagAware)
    {
        this.isGroupTagAware = isGroupTagAware;
    }

    /**
     * {@inheritDoc}
     */
    public void initialize(
        final MediaDriver.Context context,
        final UdpChannel udpChannel,
        final int initialTermId,
        final int termBufferLength)
    {
        receiverTimeoutNs = context.flowControlReceiverTimeoutNs();
        groupTag = isGroupTagAware ? context.flowControlGroupTag() : 0;
        groupMinSize = context.flowControlGroupMinSize();

        parseUriParam(udpChannel.channelUri().get(CommonContext.FLOW_CONTROL_PARAM_NAME));
    }

    /**
     * {@inheritDoc}
     */
    public long onIdle(final long timeNs, final long senderLimit, final long senderPosition, final boolean isEos)
    {
        long minLimitPosition = Long.MAX_VALUE;
        int removed = 0;
        Receiver[] receivers = this.receivers;

        for (int lastIndex = receivers.length - 1, i = lastIndex; i >= 0; i--)
        {
            final Receiver receiver = receivers[i];
            if ((receiver.timeOfLastStatusMessageNs + receiverTimeoutNs) - timeNs < 0)
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
            this.receivers = receivers;
        }

        return receivers.length < groupMinSize || receivers.length == 0 ? senderLimit : minLimitPosition;
    }

    protected final long processStatusMessage(
        final StatusMessageFlyweight flyweight,
        final long senderLimit,
        final int initialTermId,
        final int positionBitsToShift,
        final long timeNs,
        final boolean matchesTag)
    {
        final long position = computePosition(
            flyweight.consumptionTermId(),
            flyweight.consumptionTermOffset(),
            positionBitsToShift,
            initialTermId);

        final long windowLength = flyweight.receiverWindowLength();
        final long receiverId = flyweight.receiverId();
        final long lastPositionPlusWindow = position + windowLength;
        boolean isExisting = false;
        long minPosition = Long.MAX_VALUE;

        Receiver[] receivers = this.receivers;

        for (final Receiver receiver : receivers)
        {
            if (matchesTag && receiverId == receiver.receiverId)
            {
                receiver.lastPosition = Math.max(position, receiver.lastPosition);
                receiver.lastPositionPlusWindow = lastPositionPlusWindow;
                receiver.timeOfLastStatusMessageNs = timeNs;
                isExisting = true;
            }

            minPosition = Math.min(minPosition, receiver.lastPositionPlusWindow);
        }

        if (matchesTag && !isExisting)
        {
            final Receiver receiver = new Receiver(position, lastPositionPlusWindow, timeNs, receiverId);
            receivers = add(receivers, receiver);
            this.receivers = receivers;
            minPosition = Math.min(minPosition, lastPositionPlusWindow);
        }

        if (receivers.length < groupMinSize)
        {
            return senderLimit;
        }
        else if (receivers.length == 0)
        {
            return Math.max(senderLimit, lastPositionPlusWindow);
        }
        else
        {
            return Math.max(senderLimit, minPosition);
        }
    }

    public boolean hasRequiredReceivers()
    {
        return receivers.length >= groupMinSize;
    }

    protected final long receiverTimeoutNs()
    {
        return receiverTimeoutNs;
    }

    protected final boolean hasGroupTag()
    {
        return isGroupTagAware;
    }

    protected final long groupTag()
    {
        return groupTag;
    }

    protected final int groupMinSize()
    {
        return groupMinSize;
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

    private void parseUriParam(final String fcValue)
    {
        if (null != fcValue)
        {
            for (final String arg : fcValue.split(","))
            {
                if (arg.startsWith("t:"))
                {
                    receiverTimeoutNs = parseDuration("fc receiver timeout", arg.substring(2));
                }
                else if (arg.startsWith("g:"))
                {
                    final int groupMinSizeIndex = arg.indexOf('/');

                    if (2 != groupMinSizeIndex && isGroupTagAware)
                    {
                        final int lengthToParse = -1 == groupMinSizeIndex ? arg.length() - 2 : groupMinSizeIndex - 2;
                        groupTag = parseLongAscii(arg, 2, lengthToParse);
                    }

                    if (-1 != groupMinSizeIndex)
                    {
                        groupMinSize = parseIntAscii(
                            arg, groupMinSizeIndex + 1, arg.length() - (groupMinSizeIndex + 1));
                    }
                }
            }
        }
    }

    static class Receiver
    {
        long lastPosition;
        long lastPositionPlusWindow;
        long timeOfLastStatusMessageNs;
        final long receiverId;

        Receiver(final long lastPosition, final long lastPositionPlusWindow, final long timeNs, final long receiverId)
        {
            this.lastPosition = lastPosition;
            this.lastPositionPlusWindow = lastPositionPlusWindow;
            this.timeOfLastStatusMessageNs = timeNs;
            this.receiverId = receiverId;
        }
    }
}
