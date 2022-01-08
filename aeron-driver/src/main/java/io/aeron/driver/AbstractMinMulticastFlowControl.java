/*
 * Copyright 2014-2022 Real Logic Limited.
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

import java.util.Arrays;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static org.agrona.AsciiEncoding.parseIntAscii;
import static org.agrona.AsciiEncoding.parseLongAscii;
import static org.agrona.SystemUtil.parseDuration;
import static org.agrona.collections.ArrayUtil.add;

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

    private final boolean isGroupTagAware;
    private volatile boolean hasRequiredReceivers;
    private int groupMinSize;
    private long groupTag;
    private long receiverTimeoutNs;
    private Receiver[] receivers = EMPTY_RECEIVERS;
    private String channel;

    /**
     * Base constructor for use by specialised implementations.
     *
     * @param isGroupTagAware true if the group tag is used.
     */
    protected AbstractMinMulticastFlowControl(final boolean isGroupTagAware)
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
        channel = udpChannel.originalUriString();

        parseUriParam(udpChannel.channelUri().get(CommonContext.FLOW_CONTROL_PARAM_NAME));
        hasRequiredReceivers = receivers.length >= groupMinSize;
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
                receiverRemoved(
                    receiver.receiverId, receiver.sessionId, receiver.streamId, channel, receivers.length - removed);
            }
            else
            {
                minLimitPosition = Math.min(minLimitPosition, receiver.lastPositionPlusWindow);
            }
        }

        if (removed > 0)
        {
            receivers = truncateReceivers(receivers, removed);
            hasRequiredReceivers = receivers.length >= groupMinSize;
            this.receivers = receivers;
        }

        return receivers.length < groupMinSize || receivers.length == 0 ? senderLimit : minLimitPosition;
    }

    /**
     * Has the observed receiver count reached the {@link #groupMinSize()} threshold?
     *
     * @return true if the observed receiver count reached the {@link #groupMinSize()} threshold?
     */
    public boolean hasRequiredReceivers()
    {
        return hasRequiredReceivers;
    }

    /**
     * Process a received status message.
     *
     * @param flyweight           mapped over the status message.
     * @param senderLimit         the sender is currently limited to for sending.
     * @param initialTermId       for the publication.
     * @param positionBitsToShift when calculating term length with requiring a divide.
     * @param timeNs              current time.
     * @param matchesTag          if the status messages comes from a receiver with a tag matching the group.
     * @return the new position limit to be employed by the sender.
     */
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
            final Receiver receiver = new Receiver(
                receiverId, flyweight.sessionId(), flyweight.streamId(), position, lastPositionPlusWindow, timeNs);
            receivers = add(receivers, receiver);
            hasRequiredReceivers = receivers.length >= groupMinSize;
            this.receivers = receivers;
            minPosition = Math.min(minPosition, lastPositionPlusWindow);
            receiverAdded(receiver.receiverId, receiver.sessionId, receiver.streamId, channel, receivers.length);
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

    /**
     * Timeout after which an inactive receiver will be dropped.
     *
     * @return timeout after which an inactive receiver will be dropped.
     */
    protected final long receiverTimeoutNs()
    {
        return receiverTimeoutNs;
    }

    /**
     * Indicates if the flow control strategy has a group tag it is aware of for tracking membership.
     *
     * @return true if the flow control strategy has a group tag it is aware of for tracking membership.
     */
    protected final boolean hasGroupTag()
    {
        return isGroupTagAware;
    }

    /**
     * The tag used to identify members of the group.
     *
     * @return tag used to identify members of the group.
     */
    protected final long groupTag()
    {
        return groupTag;
    }

    /**
     * The minimum group size required for progress.
     *
     * @return minimum group size required for progress.
     */
    protected final int groupMinSize()
    {
        return groupMinSize;
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
            return Arrays.copyOf(receivers, newLength);
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

    private void receiverAdded(
        final long receiverId, final int sessionId, final int streamId, final String channel, final int receiverCount)
    {
//        System.out.println("Receiver added: receiverCount=" + receiverCount +
//            ", receiverId=" + receiverId + ", sessionId=" + sessionId + ", streamId=" + streamId +
//            ", channel=" + channel);
    }

    private void receiverRemoved(
        final long receiverId, final int sessionId, final int streamId, final String channel, final int receiverCount)
    {
//        System.out.println("Receiver removed: receiverCount=" + receiverCount +
//            ", receiverId=" + receiverId + ", sessionId=" + sessionId + ", streamId=" + streamId +
//            ", channel=" + channel);
    }

    static final class Receiver
    {
        final int sessionId;
        final int streamId;
        final long receiverId;
        long lastPosition;
        long lastPositionPlusWindow;
        long timeOfLastStatusMessageNs;

        Receiver(
            final long receiverId,
            final int sessionId,
            final int streamId,
            final long lastPosition,
            final long lastPositionPlusWindow,
            final long timeNs)
        {
            this.receiverId = receiverId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.lastPosition = lastPosition;
            this.lastPositionPlusWindow = lastPositionPlusWindow;
            this.timeOfLastStatusMessageNs = timeNs;
        }
    }
}
