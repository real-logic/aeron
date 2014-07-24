/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import java.net.InetSocketAddress;

public class UnicastSenderControlStrategy implements SenderControlStrategy
{
    private long positionLimit;
    private int shiftsForTermId;

    public UnicastSenderControlStrategy()
    {
        positionLimit = 0;
    }

    /** {@inheritDoc} */
    public long onStatusMessage(final long termId, final long highestContiguousSequenceNumber,
                                final long receiverWindow, final InetSocketAddress address)
    {
        final long newPositionLimit = (termId << shiftsForTermId) + receiverWindow;
        positionLimit = Math.max(positionLimit, newPositionLimit);

        return positionLimit;
    }

    /** {@inheritDoc} */
    public long initialPositionLimit(final long initialTermId, final int termBufferCapacity)
    {
        shiftsForTermId = Long.numberOfTrailingZeros(termBufferCapacity);
        positionLimit = (initialTermId << shiftsForTermId);

        return positionLimit;
    }
}
