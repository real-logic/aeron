/*
 * Copyright 2014-2025 Real Logic Limited.
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

import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Minimum multicast sender flow control strategy only for tagged members identified by a receiver tag or ASF key.
 * <p>
 * Flow control is set to minimum of tracked tagged receivers.
 * <p>
 * Tracking of tagged receivers is done as long as they continue to send Status Messages. Once SMs stop, the receiver
 * tracking for that receiver will time out after a given number of nanoseconds.
 */
public class TaggedMulticastFlowControl extends AbstractMinMulticastFlowControl
{
    /**
     * URI param value to identify this {@link FlowControl} strategy.
     */
    public static final String FC_PARAM_VALUE = "tagged";

    TaggedMulticastFlowControl()
    {
        super(true);
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
        return processStatusMessage(
            flyweight, senderLimit, initialTermId, positionBitsToShift, timeNs, matchesTag(flyweight));
    }

    /**
     * {@inheritDoc}
     */
    public void onTriggerSendSetup(
        final StatusMessageFlyweight flyweight,
        final InetSocketAddress receiverAddress,
        final long timeNs)
    {
        processSendSetupTrigger(flyweight, receiverAddress, timeNs, matchesTag(flyweight));
    }

    /**
     * {@inheritDoc}
     */
    public void onError(final ErrorFlyweight errorFlyweight, final InetSocketAddress receiverAddress, final long timeNs)
    {
        processError(errorFlyweight, receiverAddress, timeNs, matchesTag(errorFlyweight));
    }

    @SuppressWarnings("deprecation")
    private boolean matchesTag(final StatusMessageFlyweight flyweight)
    {
        final int asfLength = flyweight.asfLength();
        boolean result = false;

        if (asfLength == SIZE_OF_LONG)
        {
            if (flyweight.groupTag() == super.groupTag())
            {
                result = true;
            }
        }
        else if (asfLength >= SIZE_OF_INT)
        {
            // compatible check for ASF of first 4 bytes
            final int offset = StatusMessageFlyweight.groupTagFieldOffset();

            if (flyweight.getByte(offset) == PreferredMulticastFlowControl.PREFERRED_ASF_BYTES[0] &&
                flyweight.getByte(offset + 1) == PreferredMulticastFlowControl.PREFERRED_ASF_BYTES[1] &&
                flyweight.getByte(offset + 2) == PreferredMulticastFlowControl.PREFERRED_ASF_BYTES[2] &&
                flyweight.getByte(offset + 3) == PreferredMulticastFlowControl.PREFERRED_ASF_BYTES[3])
            {
                result = true;
            }
        }

        return result;
    }

    private boolean matchesTag(final ErrorFlyweight errorFlyweight)
    {
        return errorFlyweight.hasGroupTag() && errorFlyweight.groupTag() == super.groupTag();
    }
}
