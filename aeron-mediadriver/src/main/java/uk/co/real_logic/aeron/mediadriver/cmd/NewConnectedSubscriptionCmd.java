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
package uk.co.real_logic.aeron.mediadriver.cmd;

import uk.co.real_logic.aeron.mediadriver.DriverConnectedSubscription;
import uk.co.real_logic.aeron.mediadriver.LossHandler;
import uk.co.real_logic.aeron.mediadriver.UdpDestination;
import uk.co.real_logic.aeron.mediadriver.buffer.TermBuffers;

public class NewConnectedSubscriptionCmd
{
    private final long sessionId;
    private final long channelId;
    private final long termId;
    private final TermBuffers termBuffers;
    private final UdpDestination destination;
    private final int initialWindowSize;
    private final LossHandler lossHandler;
    private final DriverConnectedSubscription.SendSmHandler sendSmHandler;

    public NewConnectedSubscriptionCmd(final UdpDestination destination,
                                       final long sessionId,
                                       final long channelId,
                                       final long termId,
                                       final TermBuffers termBuffers,
                                       final int initialWindowSize,
                                       final LossHandler lossHandler,
                                       final DriverConnectedSubscription.SendSmHandler sendSmHandler)
    {
        this.sessionId = sessionId;
        this.channelId = channelId;
        this.termId = termId;
        this.termBuffers = termBuffers;
        this.destination = destination;
        this.initialWindowSize = initialWindowSize;
        this.lossHandler = lossHandler;
        this.sendSmHandler = sendSmHandler;
    }

    public UdpDestination destination()
    {
        return destination;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public long channelId()
    {
        return channelId;
    }

    public long termId()
    {
        return termId;
    }

    public TermBuffers termBuffers()
    {
        return termBuffers;
    }

    public int initialWindowSize()
    {
        return initialWindowSize;
    }

    public LossHandler lossHandler()
    {
        return lossHandler;
    }

    public DriverConnectedSubscription.SendSmHandler sendSmHandler()
    {
        return sendSmHandler;
    }
}
