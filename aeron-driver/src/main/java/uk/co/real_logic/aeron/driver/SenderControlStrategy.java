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

public interface SenderControlStrategy
{
    /**
     * Updates the rightEdgeOfWindow based upon new status message information
     *
     * @return the calculated rightEdgeOfWindow
     */
    long onStatusMessage(final int termId,
                         final long highestContiguousSequenceNumber,
                         final int receiverWindowSize,
                         final InetSocketAddress address);

    /**
     * Initial limit to which the position can progress.
     *
     * @param initialTermId for the term buffers
     * @param termBufferCapacity to use as the size of each term buffer
     * @return limit to which the position can go.
     */
    long initialPositionLimit(final int initialTermId, final int termBufferCapacity);
}
