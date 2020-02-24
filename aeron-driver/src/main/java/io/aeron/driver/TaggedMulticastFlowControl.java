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
import org.agrona.BitUtil;

import java.net.InetSocketAddress;

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
public class TaggedMulticastFlowControl extends AbstractMinMulticastFlowControl
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

    public TaggedMulticastFlowControl()
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
        final boolean matchesTag = matchesTag(flyweight);
        return processStatusMessage(flyweight, senderLimit, initialTermId, positionBitsToShift, timeNs, matchesTag);
    }

    private boolean matchesTag(final StatusMessageFlyweight statusMessageFlyweight)
    {
        final int asfLength = statusMessageFlyweight.asfLength();
        boolean result = false;

        if (asfLength == SIZE_OF_LONG)
        {
            if (statusMessageFlyweight.receiverTag() == super.receiverTag())
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
}
