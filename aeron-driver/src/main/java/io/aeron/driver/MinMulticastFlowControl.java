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

/**
 * Minimum multicast sender flow control strategy. Uses the {@link AbstractMinMulticastFlowControl}, but specifies that
 * the group membership for a given receiver is always <code>true</code>, so it tracks the minimum for all receivers.
 */
public class MinMulticastFlowControl extends AbstractMinMulticastFlowControl
{
    /**
     * URI param value to identify this {@link FlowControl} strategy.
     */
    public static final String FC_PARAM_VALUE = "min";

    MinMulticastFlowControl()
    {
        super(false);
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
        return processStatusMessage(flyweight, senderLimit, initialTermId, positionBitsToShift, timeNs, true);
    }

    /**
     * {@inheritDoc}
     */
    public void onTriggerSendSetup(
        final StatusMessageFlyweight flyweight,
        final InetSocketAddress receiverAddress,
        final long timeNs)
    {
        processSendSetupTrigger(flyweight, receiverAddress, timeNs, true);
    }

    /**
     * {@inheritDoc}
     */
    public void onError(final ErrorFlyweight errorFlyweight, final InetSocketAddress receiverAddress, final long timeNs)
    {
        processError(errorFlyweight, receiverAddress, timeNs, true);
    }
}
