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
    private long rightEdgeOfWindow;
    private int shiftsForTermId;

    public UnicastSenderControlStrategy()
    {
        rightEdgeOfWindow = 0;
    }

    /** {@inheritDoc} */
    public long onStatusMessage(final long termId, final long highestContiguousSequenceNumber,
                                final long receiverWindow, final InetSocketAddress address)
    {
        final long newRightEdgeOfWindow = (termId << shiftsForTermId) + receiverWindow;
        rightEdgeOfWindow = Math.max(rightEdgeOfWindow, newRightEdgeOfWindow);

        return rightEdgeOfWindow;
    }

    /** {@inheritDoc} */
    public long initialRightEdge(final long initialTermId, final int sizeOfTermBuffer)
    {
        shiftsForTermId = Long.numberOfTrailingZeros(sizeOfTermBuffer);
        rightEdgeOfWindow = (initialTermId << shiftsForTermId);

        return rightEdgeOfWindow;
    }
}
