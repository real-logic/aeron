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
import org.agrona.AsciiEncoding;
import org.agrona.BitUtil;
import org.agrona.SystemUtil;

import java.net.InetSocketAddress;

import static io.aeron.driver.MinMulticastFlowControl.EMPTY_RECEIVERS;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static java.lang.System.getProperty;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Minimum multicast sender flow control strategy only for tagged members identified by a receiver tag or ASF key.
 * <p>
 * Flow control is set to minimum of tracked tagged receivers.
 * <p>
 * Tracking of tagged receivers is done as long as they continue to send Status Messages. Once SMs stop, the receiver
 * tracking for that receiver will timeout after a given number of nanoseconds.
 */
public class TaggedMulticastFlowControl implements FlowControl
{
    /**
     * URI param value to identify this {@link FlowControl} strategy.
     */
    public static final String FC_PARAM_VALUE = "tagged";

    /**
     * Property name used to set Application Specific Feedback (ASF) in Status Messages to identify preferred receivers.
     */
    public static final String PREFERRED_ASF_PROP_NAME = Configuration.PREFERRED_ASF_PROP_NAME;

    /**
     * Default Application Specific Feedback (ASF) value
     */
    public static final String PREFERRED_ASF_DEFAULT = Configuration.PREFERRED_ASF_DEFAULT;

    public static final String PREFERRED_ASF = getProperty(PREFERRED_ASF_PROP_NAME, PREFERRED_ASF_DEFAULT);
    public static final byte[] PREFERRED_ASF_BYTES = BitUtil.fromHex(PREFERRED_ASF);

    private volatile MinMulticastFlowControl.Receiver[] receivers = EMPTY_RECEIVERS;
    private int requiredGroupSize;
    private long receiverTimeoutNs;
    private long receiverTag;

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
        receiverTag = context.flowControlGroupReceiverTag();
        requiredGroupSize = context.flowControlReceiverGroupMinSize();

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
                    final int requiredGroupSizeIndex = arg.indexOf('/');

                    if (2 != requiredGroupSizeIndex)
                    {
                        final int lengthToParse = -1 == requiredGroupSizeIndex ?
                            arg.length() - 2 : requiredGroupSizeIndex - 2;
                        receiverTag = AsciiEncoding.parseLongAscii(arg, 2, lengthToParse);
                    }

                    if (-1 != requiredGroupSizeIndex)
                    {
                        requiredGroupSize = AsciiEncoding.parseIntAscii(
                            arg, requiredGroupSizeIndex + 1, arg.length() - (requiredGroupSizeIndex + 1));
                    }
                }
            }
        }
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
        final boolean isTagged = isTagged(flyweight);
        final long lastPositionPlusWindow = position + windowLength;
        boolean isExisting = false;
        long minPosition = Long.MAX_VALUE;

        MinMulticastFlowControl.Receiver[] receivers = this.receivers;

        for (final MinMulticastFlowControl.Receiver receiver : receivers)
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
            final MinMulticastFlowControl.Receiver receiver = new MinMulticastFlowControl.Receiver(
                position, lastPositionPlusWindow, timeNs, receiverId);
            receivers = MinMulticastFlowControl.add(receivers, receiver);
            this.receivers = receivers;
            minPosition = Math.min(minPosition, lastPositionPlusWindow);
        }

        if (receivers.length < requiredGroupSize)
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
     * {@inheritDoc}
     */
    public long onIdle(final long timeNs, final long senderLimit, final long senderPosition, final boolean isEos)
    {
        long minLimitPosition = Long.MAX_VALUE;
        int removed = 0;
        MinMulticastFlowControl.Receiver[] receivers = this.receivers;

        for (int lastIndex = receivers.length - 1, i = lastIndex; i >= 0; i--)
        {
            final MinMulticastFlowControl.Receiver receiver = receivers[i];
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
            receivers = MinMulticastFlowControl.truncateReceivers(receivers, removed);
            this.receivers = receivers;
        }

        return receivers.length < requiredGroupSize || receivers.length == 0 ? senderLimit : minLimitPosition;
    }

    public boolean hasRequiredReceivers()
    {
        return receivers.length >= requiredGroupSize;
    }

    private boolean isTagged(final StatusMessageFlyweight statusMessageFlyweight)
    {
        final int asfLength = statusMessageFlyweight.asfLength();
        boolean result = false;

        if (asfLength == SIZE_OF_LONG)
        {
            if (statusMessageFlyweight.receiverTag() == receiverTag)
            {
                result = true;
            }
        }
        else if (asfLength >= SIZE_OF_INT)
        {
            // compatible check for ASF of first 4 bytes
            final int offset = StatusMessageFlyweight.receiverTagFieldOffset();

            if (statusMessageFlyweight.getByte(offset) == PREFERRED_ASF_BYTES[0] &&
                statusMessageFlyweight.getByte(offset + 1) == PREFERRED_ASF_BYTES[1] &&
                statusMessageFlyweight.getByte(offset + 2) == PREFERRED_ASF_BYTES[2] &&
                statusMessageFlyweight.getByte(offset + 3) == PREFERRED_ASF_BYTES[3])
            {
                result = true;
            }
        }

        return result;
    }

    long receiverTag()
    {
        return receiverTag;
    }

    long receiverTimeoutNs()
    {
        return receiverTimeoutNs;
    }

    int requiredGroupSize()
    {
        return requiredGroupSize;
    }
}
