package io.aeron.driver;

import io.aeron.CommonContext;
import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.AsciiEncoding;
import org.agrona.SystemUtil;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;

public abstract class TaggedMulticastFlowControlBase implements FlowControl
{
    private static final Receiver[] EMPTY_RECEIVERS = new Receiver[0];

    private volatile Receiver[] receivers = EMPTY_RECEIVERS;

    private long receiverTimeoutNs;
    private int groupMinSize;
    private long groupReceiverTag;

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
        groupReceiverTag = context.flowControlGroupReceiverTag();
        groupMinSize = context.flowControlReceiverGroupMinSize();

        final String fcValue = udpChannel.channelUri().get(CommonContext.FLOW_CONTROL_PARAM_NAME);

        if (null != fcValue)
        {
            for (final String arg : fcValue.split(","))
            {
                if (arg.startsWith("t:"))
                {
                    receiverTimeoutNs = SystemUtil.parseDuration("fc receiver timeout", arg.substring(2));
                }
                else if (arg.startsWith("g:"))
                {
                    final int groupMinSizeIndex = arg.indexOf('/');

                    if (2 != groupMinSizeIndex)
                    {
                        final int lengthToParse = -1 == groupMinSizeIndex ?
                            arg.length() - 2 : groupMinSizeIndex - 2;
                        groupReceiverTag = AsciiEncoding.parseLongAscii(arg, 2, lengthToParse);
                    }

                    if (-1 != groupMinSizeIndex)
                    {
                        groupMinSize = AsciiEncoding.parseIntAscii(
                            arg, groupMinSizeIndex + 1, arg.length() - (groupMinSizeIndex + 1));
                    }
                }
            }
        }
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

    public boolean hasRequiredReceivers()
    {
        return receivers.length >= groupMinSize;
    }

    protected long handleStatusMessage(
        final StatusMessageFlyweight flyweight,
        final boolean isTagged,
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
        final long lastPositionPlusWindow = position + windowLength;
        boolean isExisting = false;
        long minPosition = Long.MAX_VALUE;

        Receiver[] receivers = this.receivers;

        for (final Receiver receiver : receivers)
        {
            if (isTagged && receiverId == receiver.receiverId)
            {
                receiver.lastPosition = Math.max(position, receiver.lastPosition);
                receiver.lastPositionPlusWindow = lastPositionPlusWindow;
                receiver.timeOfLastStatusMessageNs = timeNs;
                isExisting = true;
            }

            minPosition = Math.min(minPosition, receiver.lastPositionPlusWindow);
        }

        if (isTagged && !isExisting)
        {
            final Receiver receiver = new Receiver(
                position, lastPositionPlusWindow, timeNs, receiverId);
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

    private static class Receiver
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

    private static Receiver[] add(final Receiver[] receivers, final Receiver receiver)
    {
        final int length = receivers.length;
        final Receiver[] newElements = new Receiver[length + 1];

        System.arraycopy(receivers, 0, newElements, 0, length);
        newElements[length] = receiver;
        return newElements;
    }

    private static Receiver[] truncateReceivers(final Receiver[] receivers, final int removed)
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

    long receiverTag()
    {
        return groupReceiverTag;
    }

    long receiverTimeoutNs()
    {
        return receiverTimeoutNs;
    }

    int groupMinSize()
    {
        return groupMinSize;
    }
}
